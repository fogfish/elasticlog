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
   stream/2
   % sigma/1
]).


stream(Keys, Head) ->
   fun(Sock) ->
      [identity ||
         schema(Sock, Keys),
         lists:zip3(_, Keys, Head),
         q(_),
         esio:stream(Sock, _),
         head(Keys, _)
      ]
   end.

schema(Sock, Keys) ->
   {ok, Schema} = esio:schema(Sock),
   {_,  Bucket} = hd(maps:to_list(Schema)),
   [lens:get(lens_schema(Key), Bucket) || Key <- Keys].

lens_schema(Key) ->
   lens:c(
      lens:at(<<"mappings">>), 
      lens:at(<<"_doc">>),
      lens:at(<<"properties">>),
      lens:at(Key),
      lens:at(<<"type">>)
   ).

head(Keys, Stream) ->
   stream:map(
      fun(#{<<"_source">> := Json, <<"_score">> := Score}) ->
         lists:map(fun(X) -> lens:get(lens:at(X), Json) end, Keys)
      end,
      Stream
   ).


%%
%% build sigma function
% sigma(Pattern) ->
%    fun(Sock) ->
%       fun(Stream) ->
%          sigma(Sock, datalog:bind(stream:head(Stream), Pattern))
%       end
%    end. 

% sigma(Sock, #{'_' := Head} = Pattern) ->
%    heap(Head, Pattern, esio:stream(Sock, q(Pattern))).

%%
%%
q(Pattern) ->
   io:format("==> ~p~n", [Pattern]),

   Matches = lists:flatten(lists:map(fun(X) -> match(X) end, Pattern)),
   Filters = lists:flatten(lists:map(fun(X) -> filter(X) end, Pattern)),
   debug(#{'query' => #{bool => #{must => Matches, filter => Filters}}}).

%%
%%
debug(Json) ->
   io:format("~s~n", [jsx:encode(Json)]),
   Json.

%%
%%
match({_, ElasticKey, '_'}) ->
   [#{exists => #{field => ElasticKey}}];
match({<<"text">>, ElasticKey, Pattern}) -> 
   elastic_query_string(ElasticKey, Pattern);
% match({Type, ElasticKey, Pattern}) ->
%    elastic_match(Type, ElasticKey, Pattern);
match(_) ->
   [].

%%
%%
filter({_, _, '_'}) ->
   [];
% filter({<<"geo_point">>, ElasticKey, Pattern}) ->
%    elastic_geo_distance(ElasticKey, Pattern);
% filter({Type, ElasticKey, Pattern}) ->
%    elastic_filter(Type, ElasticKey, Pattern);
filter(_) ->
   [].


%%
%%
elastic_query_string(ElasticKey, Value)
 when not is_list(Value) ->
   %% range filter is encoded as list at datalog: [{'>', ...}, ...]
   [#{query_string => #{default_field => ElasticKey, 'query' => Value}}];

elastic_query_string(ElasticKey, [H | _] = Value)
 when not is_tuple(H) ->
   [#{query_string => #{default_field => ElasticKey, 'query' => scalar:s(lists:join(<<" AND ">>, Value))}}];

elastic_query_string(_, _) ->
   []. 


%  when is_atom(DatalogKey)  ->
%    case Pattern of
%       %% range filter is encoded as list at datalog: [{'>', ...}, ...]
%       #{DatalogKey := Value} when not is_list(Value) ->
%          [#{query_string => #{default_field => ElasticKey, 'query' => Value}}];
%       #{DatalogKey := [H | _] = Value} when not is_tuple(H) ->
%          [#{query_string => #{default_field => ElasticKey, 'query' => scalar:s(lists:join(<<" AND ">>, Value))}}];
%       _ ->
%          [#{exists => #{field => ElasticKey}}]
%    end;

% elastic_query_string(DatalogVal, ElasticKey, _) ->
%    [#{query_string => #{default_field => ElasticKey, 'query' => DatalogVal}}].


%%
%%
elastic_match(Type, DatalogKey, ElasticKey, Pattern)
 when is_atom(DatalogKey)  ->
   case Pattern of
      %% range filter is encoded as list at datalog: [{'>', ...}, ...]
      #{DatalogKey := Value} when not is_list(Value) ->
         [#{match => #{ElasticKey => elasticlog_codec:encode(Type, Value)}}];
      #{DatalogKey := [H | _] = Value} when not is_tuple(H) ->
         [#{match => #{ElasticKey => elasticlog_codec:encode(Type, X)}} || X <- Value];
      _ ->
         [#{exists => #{field => ElasticKey}}]
   end;

elastic_match(_Type, DatalogVal, ElasticKey, _) ->
   [#{match => #{ElasticKey => DatalogVal}}].



%%
%%
elastic_filter(Type, DatalogKey, ElasticKey, Pattern) ->
   case Pattern of
      %% range filter is encoded as list at datalog: [{'>', ...}, ...]
      #{DatalogKey := [H | _] = Value} when is_tuple(H) ->
         #{range => 
            #{ElasticKey => maps:from_list([{elastic_compare(Op), elasticlog_codec:encode(Type, X)} || {Op, X} <- Value])}
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
   P = [{X, Y, scalar:s(Type)} || #{'@' := Type, '_' := [_, X, Y | _]} <- Seq, lists:member(Y, Head)],
   K = [X || #{'_' := [_, _, _, X]} <- Seq, lists:member(X, Head)],
   stream:map(
      fun(#{<<"_source">> := Json, <<"_score">> := Score}) ->
         decode_k(Score, decode_p(Json, decode_s(Json, #{}, S), P), K)
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

decode_k(Score, Acc0, K) ->
   lists:foldl(
      fun(HeapKey, Acc) -> 
         ErlangVal = elasticlog_codec:decode(?XSD_DECIMAL, Score),
         Acc#{HeapKey => ErlangVal} 
      end, 
      Acc0, 
      K
   ).

