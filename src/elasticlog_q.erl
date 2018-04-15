%%
%%   Copyright 2016 Dmitry Kolesnikov, All Rights Reserved
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
%% @doc
%%   datalog sigma function supports knowledge statement only.
-module(elasticlog_q).

-compile({parse_transform, category}).
-include_lib("semantic/include/semantic.hrl").

-export([
   sigma/1
]).

%%
%% build sigma function
sigma(Pattern) ->
   fun(Sock) ->
      fun(Stream) ->
         sigma(Sock, datalog:bind(stream:head(Stream), Pattern))
      end
   end. 

sigma(Sock, #{'_' := Head} = Pattern) ->
   heap(Head, Pattern, esio:stream(Sock, q(Pattern))).

%%
%%
q(#{'@' := Seq} = Pattern) ->
   Matches = lists:flatten(lists:map(fun(X) -> match(maps:merge(Pattern, X)) end, Seq)),
   Filters = lists:flatten(lists:map(fun(X) -> filter(maps:merge(Pattern, X)) end, Seq)),
   #{'query' => #{bool => #{must => Matches, filter => Filters}}}.

%%
%%
% debug(Json) ->
%    io:format("~s~n", [jsx:encode(Json)]),
%    Json.

%%
%%
match(#{'@' := 'xsd:string', '_' := [_, P, O | _]} = Pattern) ->
   elastic_query_string(O, P, Pattern);

match(#{'@' := _, '_' := [_, P, O | _]} = Pattern) ->
   elastic_match(O, P, Pattern);

match(_) ->
   [].

%%
%%
filter(#{'@' := 'georss:hash', '_' := [_, P, O | _]} = Pattern) ->
   elastic_geo_distance(O, P, Pattern);

filter(#{'@' := _, '_' := [_, P, O | _]} = Pattern) ->
   elastic_filter(O, P, Pattern);

filter(_) ->
   [].


%%
%%
elastic_match(DatalogKey, ElasticKey, Pattern)
 when is_atom(DatalogKey)  ->
   case Pattern of
      %% range filter is encoded as list at datalog: [{'>', ...}, ...]
      #{DatalogKey := Value} when not is_list(Value) ->
         [#{match => #{ElasticKey => Value}}];
      _ ->
         []
   end;

elastic_match(DatalogVal, ElasticKey, _) ->
   [#{match => #{ElasticKey => DatalogVal}}].

%%
%%
elastic_query_string(DatalogKey, ElasticKey, Pattern)
 when is_atom(DatalogKey)  ->
   case Pattern of
      %% range filter is encoded as list at datalog: [{'>', ...}, ...]
      #{DatalogKey := Value} when not is_list(Value) ->
         [#{query_string => #{default_field => ElasticKey, 'query' => Value}}];
      _ ->
         []
   end;

elastic_query_string(DatalogVal, ElasticKey, _) ->
   [#{query_string => #{default_field => ElasticKey, 'query' => DatalogVal}}].


%%
%%
elastic_filter(DatalogKey, ElasticKey, Pattern) ->
   case Pattern of
      %% range filter is encoded as list at datalog: [{'>', ...}, ...]
      #{DatalogKey := Value} when is_list(Value) ->
         #{range => 
            #{ElasticKey => maps:from_list([{elastic_compare(Op), X} || {Op, X} <- Value])}
         };
      _ ->
         []
   end.

%%
%%
elastic_compare('>')  -> gt;
elastic_compare('>=') -> gte; 
elastic_compare('<')  -> lt;
elastic_compare('=<') -> lte.


%%
%%
elastic_geo_distance(DatalogKey, ElasticKey, Pattern) ->
   case Pattern of
      %% geo range filter is encoded as list at datalog: [hash, radius]
      #{DatalogKey := [GeoHash, Radius]} ->
         #{geo_distance => #{distance => Radius, ElasticKey => GeoHash}};
      _ ->
         []
   end.


%%
%%
heap(Head, #{'@' := Seq}, Stream) ->
   S = [X || #{'_' := [X | _]} <- Seq, lists:member(X, Head)],
   P = [{X, Y, scalar:s(Type)} || #{'@' := Type, '_' := [_, X, Y]} <- Seq, lists:member(Y, Head)],
   stream:map(
      fun(#{<<"_source">> := Json}) ->
         decode_p(Json, decode_s(Json, #{}, S), P)
      end,
      Stream
   ).

decode_s(Json, Acc0, S) ->
   lists:foldl(
      fun(HeapKey, Acc) -> 
         ElasticVal = lens:get(lens:at(<<"s">>), Json),
         ErlangVal = elasticlog_codec:decode(?XSD_ANYURI, ElasticVal),
         Acc#{HeapKey => ErlangVal} 
      end, 
      Acc0, 
      S
   ).

decode_p(Json, Acc0, P) ->
   lists:foldl(
      fun({ElasticKey, HeapKey, Type}, Acc) ->
         ElasticVal = lens:get(lens:at(ElasticKey), Json),
         ErlangVal = elasticlog_codec:decode(Type, ElasticVal),
         Acc#{HeapKey => ErlangVal}
      end,
      Acc0,
      P
   ).
