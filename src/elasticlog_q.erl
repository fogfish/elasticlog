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
   io:format("==> ~p~n", [q(Pattern)]),
   heap(Head, esio:stream(Sock, q(Pattern))).

%%
%%
q(Pattern) ->
   Matches = lists:flatten([
      match_sp(Pattern),
      match_o(Pattern)
   ]),
   Filters  = lists:flatten([
      filter_o(Pattern),
      filter_c(Pattern),
      filter_k(Pattern)
   ]),
   #{'query' => #{bool => #{must => Matches, filter => Filters}}}.

%%
%%
match_sp(#{'_' := [S, P | _]} = Pattern) ->
   [elastic_match(S, s, Pattern), elastic_match(P, p, Pattern)].

%%
%%
match_o(#{'@' := Type, '_' := [_, _, O | _]} = Pattern) ->
   elastic_match(O, elastic_key(Type), Pattern);

match_o(_) ->
   [].

%%
%%
filter_o(#{'@' := Type, '_' := [_, _, O | _]} = Pattern) ->
   elastic_filter(O, elastic_key(Type), Pattern);

filter_o(_) ->
   [].

%%
%%
filter_c(#{'_' := [_, _, _, C | _]} = Pattern) ->
   elastic_filter(C, c, Pattern);

filter_c(_) ->
   [].

%%
%%
filter_k(#{'_' := [_, _, _, _, K | _]} = Pattern) ->
   elastic_filter(K, k, Pattern);

filter_k(_) ->
   [].

%%
%%
elastic_key('xsd:anyURI') -> xsd_anyuri;
elastic_key('xsd:string') -> xsd_string;
elastic_key('xsd:integer') -> xsd_integer;
elastic_key('xsd:long') -> xsd_long;
elastic_key('xsd:int') -> xsd_int;
elastic_key('xsd:short') -> xsd_short;
elastic_key('xsd:byte') -> xsd_byte;
elastic_key('xsd:decimal') -> xsd_decimal;
elastic_key('xsd:float') -> xsd_float;
elastic_key('xsd:double') -> xsd_double;
elastic_key('xsd:boolean') -> xsd_boolean;
elastic_key('xsd:datetime') -> xsd_datetime;
elastic_key('xsd:date') -> xsd_date;
elastic_key('xsd:time') -> xsd_time;
elastic_key('xsd:yearmonth') -> xsd_yearmonth;
elastic_key('xsd:year') -> xsd_year;
elastic_key('georss:point') -> georss_point;
elastic_key('georss:hash') -> georss_hash.

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
heap([S, P, O], Stream) ->
   stream:map(
      fun(#{<<"_source">> := Json}) ->
         #{s := Sval, p := Pval, o := Oval} = elasticlog_codec:decode(Json),
         #{S => Sval, P => Pval, O => Oval} 
      end,
      Stream
   );

heap([S, P, O, C], Stream) ->
   stream:map(
      fun(#{<<"_source">> := Json, <<"_score">> := Cval}) ->
         #{s := Sval, p := Pval, o := Oval} = elasticlog_codec:decode(Json),
         #{S => Sval, P => Pval, O => Oval, C => Cval} 
      end,
      Stream
   );

heap([S, P, O, C, K], Stream) ->
   stream:map(
      fun(#{<<"_source">> := Json, <<"_score">> := Cval}) ->
         #{s := Sval, p := Pval, o := Oval, k := Kval} = elasticlog_codec:decode(Json),
         #{S => Sval, P => Pval, O => Oval, C => Cval, K => Kval} 
      end,
      Stream
   ).

