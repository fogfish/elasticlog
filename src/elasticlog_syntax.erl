%% @doc
%%   elastic syntax helper
-module(elasticlog_syntax).
-include_lib("semantic/include/semantic.hrl").

-export([
   keys/1
,  pattern/1
,  aggregate/1
]).

%%
%% we support an optional keys as part of logical statement
keys([<<$?, Key/binary>> | Keys]) ->
   [{option, Key} | keys(Keys)];
keys([Key | Keys]) ->
   [{required, Key} | keys(Keys)];
keys([]) ->
   [].

%%
%% Build a pattern match elastic search query
pattern(Pattern) ->
   Matches = lists:flatten(lists:map(fun(X) -> match(X) end, Pattern)),
   Filters = lists:flatten(lists:map(fun(X) -> filter(X) end, Pattern)),
   #{'query' => #{bool => #{must => Matches, filter => Filters}}}.



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
filter({_, _, '_'}) ->
   [];
filter({?GEORSS_HASH, {_, ElasticKey}, Pattern}) ->
   elastic_geo_distance(ElasticKey, Pattern);
filter({Type, {_, ElasticKey}, Pattern}) ->
   elastic_filter(Type, ElasticKey, Pattern);
filter(_) ->
   [].



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
elastic_compare('>')  -> gt;
elastic_compare('>=') -> gte; 
elastic_compare('<')  -> lt;
elastic_compare('=<') -> lte.


%%
elastic_geo_distance(ElasticKey, [GeoHash, Radius]) ->
   %% geo range filter is encoded as list at datalog: [hash, radius]
   #{geo_distance => #{distance => Radius, ElasticKey => GeoHash}};

elastic_geo_distance(_, _) ->
   [].


%%
%%
aggregate(#{'@' := category, '_' := [Key]}) ->
   {bucket, Key,
      #{
         Key => #{
            terms => #{
               field => Key
            }
         }
      }
   };

aggregate(#{'@' := category, '_' := [N, Key]}) ->
   {bucket, Key,
      #{
         Key => #{
            terms => #{
               field => Key,
               size  => N
            }
         }
      }
   };

aggregate(#{'@' := sum, '_' := [Key]}) ->
   {metric, Key,
      #{
         Key => #{
            sum => #{
               field => Key
            }
         }
      }
   }.
