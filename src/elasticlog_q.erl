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

-include("elasticlog.hrl").
-include_lib("semantic/include/semantic.hrl").

-export([stream/2]).


stream([<<"ENV">> | Key], _Head) ->
   environment(Key);

stream([Bucket | Keys], Head) ->
   stream(Bucket, elastic_keys(Keys), Head).

elastic_keys([<<$?, Key/binary>> | Keys]) ->
   [{option, Key} | elastic_keys(Keys)];
elastic_keys([Key | Keys]) ->
   [{required, Key} | elastic_keys(Keys)];
elastic_keys([]) ->
   [].

stream(Bucket, Keys, Head) ->
   fun(#elasticlog{sock = Sock}) ->
      [identity ||
         schema(Sock, Keys),
         Schema <- lists:zip3(_, Keys, Head),
         q(Schema),
         esio:stream(Sock, Bucket, _),
         head(Schema, _)
      ]
   end.

schema(Sock, Keys) ->
   {ok, Schema} = elasticlog:schema(Sock),
   [maps:get(Key, Schema) || {_, Key} <- Keys].

head(Schema, Stream) ->
   stream:map(
      fun(#{<<"_source">> := Json, <<"_score">> := _Score}) ->
         lists:map(
            fun({Type, {_, Key}, _}) ->
               elasticlog_codec:decode(Type, lens:get(lens:at(Key), Json))
            end,
            Schema
         )
      end,
      Stream
   ).

%%
%%
q(Pattern) ->
   Matches = lists:flatten(lists:map(fun(X) -> match(X) end, Pattern)),
   Filters = lists:flatten(lists:map(fun(X) -> filter(X) end, Pattern)),
   #{'query' => #{bool => #{must => Matches, filter => Filters}}}.

%%
%%
% debug(Json) ->
%    error_logger:info_msg("[elasticlog] query: ~s~n", [jsx:encode(Json)]),
%    Json.

%%
%%
match({_, {required, ElasticKey}, '_'}) ->
   [#{exists => #{field => ElasticKey}}];
match({_, {option, _}, '_'}) ->
   [];
match({_, {required, ElasticKey}, undefined}) ->
   [#{exists => #{field => ElasticKey}}];
match({_, {option, _}, undefined}) ->
   [];
match({?GEORSS_HASH, _, _}) ->
   %% Geo fields do not support exact searching, use dedicated geo queries instead
   [];
match({?XSD_STRING, {_, ElasticKey}, Pattern}) -> 
   elastic_query_string(ElasticKey, Pattern);
match({Type, {_, ElasticKey}, Pattern}) ->
   elastic_match(Type, ElasticKey, Pattern);
match(_) ->
   [].

%%
%%
filter({_, _, '_'}) ->
   [];
filter({?GEORSS_HASH, {_, ElasticKey}, Pattern}) ->
   elastic_geo_distance(ElasticKey, Pattern);
filter({Type, {_, ElasticKey}, Pattern}) ->
   elastic_filter(Type, ElasticKey, Pattern);
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


%%
%%
elastic_match(Type, ElasticKey, Value)
 when not is_list(Value) ->
   %% range filter is encoded as list at datalog: [{'>', ...}, ...]
   [#{match => #{ElasticKey => elasticlog_codec:encode(Type, Value)}}];

elastic_match(Type, ElasticKey, [H | _] = Value)
 when not is_tuple(H) ->
   [#{match => #{ElasticKey => elasticlog_codec:encode(Type, X)}} || X <- Value];

elastic_match(_, _, _) ->
   [].


%%
%%
elastic_filter(Type, ElasticKey, [H | _] = Value)
 when is_tuple(H) ->
   %% range filter is encoded as list at datalog: [{'>', ...}, ...]
   #{range => 
      #{ElasticKey => maps:from_list([{elastic_compare(Op), elasticlog_codec:encode(Type, X)} || {Op, X} <- Value])}
   };

elastic_filter(_, _, _) ->
   [].


%%
%%
elastic_compare('>')  -> gt;
elastic_compare('>=') -> gte; 
elastic_compare('<')  -> lt;
elastic_compare('=<') -> lte.

%%
%%
elastic_geo_distance(ElasticKey, [GeoHash, Radius]) ->
   %% geo range filter is encoded as list at datalog: [hash, radius]
   #{geo_distance => #{distance => Radius, ElasticKey => GeoHash}};

elastic_geo_distance(_, _) ->
   [].

%%
%%
environment(Keys) ->
   fun(#elasticlog{env = Env}) ->
      stream:new([maps:get(Key, Env) || Key <- Keys])
   end.
