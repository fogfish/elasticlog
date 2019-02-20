%% @doc
%%   elastic syntax helper
-module(elasticlog_syntax).
-include_lib("semantic/include/semantic.hrl").

-export([
   keys/2
,  is_aggregate/1
,  pattern/2
,  aggregate/1
]).

%%
%% we support an optional keys as part of logical statement
keys(Keys, undefined) ->
   keys(Keys);
keys(Keys, Equiv) ->
   keys([equivalent(X, Equiv) || X <- Keys]).

keys([{option, _} = Key | Keys]) ->
   [Key | keys(Keys)];
keys([{sortby, _} = Key | Keys]) ->
   [Key | keys(Keys)];
keys([{required, _} = Key | Keys]) ->
   [Key | keys(Keys)];
keys([Key | Keys]) when is_binary(Key) ->
   [{required, Key} | keys(Keys)];
keys([{_, Key} | Keys]) ->
   [{required, Key} | keys(Keys)];
%% Note: following patterns are required to implement aggregations
keys([{_, _, Key} | Keys]) ->
   [{required, Key} | keys(Keys)];
keys([{_, _, _, Key} | Keys]) ->
   [{required, Key} | keys(Keys)];
keys([{_, _, _, _, Key} | Keys]) ->
   [{required, Key} | keys(Keys)];
keys([{_, _, _, _, _, Key} | Keys]) ->
   [{required, Key} | keys(Keys)];
keys([]) ->
   [].

equivalent({Option, Key}, Equiv) ->
   {Option, maps:get(Key, Equiv, Key)};
equivalent(Key, Equiv) ->
   maps:get(Key, Equiv, Key).

%%
%%
is_aggregate([{option, _} | Keys]) ->
   is_aggregate(Keys);
is_aggregate([{sortby, _} | Keys]) ->
   is_aggregate(Keys);
is_aggregate([{required, _} | Keys]) ->
   is_aggregate(Keys);
is_aggregate([Key | Keys]) when is_binary(Key) ->
   is_aggregate(Keys);
is_aggregate([_ | _]) ->
   true;
is_aggregate([]) ->
   false.

%%
%% Build a pattern match elastic search query
pattern(Pattern, Implicit) ->
   Matches = lists:flatten(lists:map(fun(X) -> match(X) end, Pattern) ++ implicitly(Implicit)),
   Negates = lists:flatten(lists:map(fun(X) -> negate(X) end, Pattern)),
   Filters = lists:flatten(lists:map(fun(X) -> filter(X) end, Pattern)),
   case Negates of
      [] ->
         #{'query' => #{bool => #{must => Matches, filter => Filters}}};
      _  ->
         #{'query' => #{bool => #{must => Matches, must_not => Negates, filter => Filters}}}
   end.


%%
%%
match({_, {required, ElasticKey}, '_'}) ->
   [#{exists => #{field => ElasticKey}}];
match({_, {sortby, ElasticKey}, '_'}) ->
   [#{exists => #{field => ElasticKey}}];
match({_, {option, _}, '_'}) ->
   [];
match({_, {required, ElasticKey}, undefined}) ->
   [#{exists => #{field => ElasticKey}}];
match({_, {sortby, ElasticKey}, undefined}) ->
   [#{exists => #{field => ElasticKey}}];
match({_, {option, _}, undefined}) ->
   [];
%%
%% Geo fields do not support exact searching, we are using geo filters
match({?GEORSS_HASH, _, _}) ->
   [];
match({?GEORSS_POINT, _, _}) ->
   [];
match({?GEORSS_JSON, _, _}) ->
   [];
match({?XSD_STRING, {_, ElasticKey}, Pattern}) -> 
   elastic_query_string(ElasticKey, Pattern);
match({Type, {_, ElasticKey}, #{<<"key">> := IRI}}) ->
   %% Note: this is special case to support join via aggregated value
   elastic_match(Type, ElasticKey, {iri, IRI});
match({Type, {_, ElasticKey}, Pattern}) ->
   elastic_match(Type, ElasticKey, Pattern);
match(_) ->
   [].

%%
%%
negate({Type, {_, ElasticKey}, [{'=/=', Value}]}) ->
   elastic_not_match(Type, ElasticKey, Value);
negate(_) ->
   [].


%%
%%
implicitly(undefined) ->
   [];
implicitly(Implicit) ->
   [#{match => #{ElasticKey => Value}} || {ElasticKey, Value} <- maps:to_list(Implicit)].

%%
%%
elastic_query_string(ElasticKey, {iri, _} = Pattern) ->
   %% sometimes elastic schema is mis-configured
   elastic_match(?XSD_ANYURI, ElasticKey, Pattern);

elastic_query_string(ElasticKey, Value)
 when not is_list(Value) ->
   %% range filter is encoded as list at datalog: [{'>', ...}, ...]
   [#{query_string => #{default_field => ElasticKey, 'query' => Value}}];

elastic_query_string(ElasticKey, [H | _] = Value)
 when not is_tuple(H) ->
   [#{query_string => #{default_field => ElasticKey, 'query' => typecast:s(lists:join(<<" AND ">>, Value))}}];

elastic_query_string(_, _) ->
   []. 



%%
elastic_match(_Type, ElasticKey, Value)
 when not is_list(Value) ->
   %% range filter is encoded as list at datalog: [{'>', ...}, ...]
   [#{match => #{ElasticKey => semantic:to_json(Value)}}];

elastic_match(_Type, ElasticKey, [H | _] = Value)
 when not is_tuple(H) ->
   [#{match => #{ElasticKey => semantic:to_json(X)}} || X <- Value];

elastic_match(_, _, _) ->
   [].

%%
elastic_not_match(_Type, ElasticKey, Value) ->
   [#{term => #{ElasticKey => semantic:to_json(Value)}}].


%%
%%
filter({_, _, '_'}) ->
   [];
filter({?GEORSS_HASH, {_, ElasticKey}, Pattern}) ->
   elastic_geo_distance(ElasticKey, Pattern);
filter({?GEORSS_POINT, {_, ElasticKey}, Pattern}) ->
   elastic_geo_distance(ElasticKey, Pattern);
filter({?GEORSS_JSON, {_, ElasticKey}, Pattern}) ->
   elastic_geo_shape(ElasticKey, Pattern);
filter({Type, {_, ElasticKey}, Pattern}) ->
   elastic_filter(Type, ElasticKey, Pattern);
filter(_) ->
   [].



%%
elastic_filter(_Type, ElasticKey, [H | _] = Value)
 when is_tuple(H) ->
   %% range filter is encoded as list at datalog: [{'>', ...}, ...]
   #{range => 
      #{ElasticKey => maps:from_list([{elastic_compare(Op), semantic:to_json(X)} || {Op, X} <- Value, Op /= '=/='])}
   };

elastic_filter(_, _, _) ->
   [].



%%
elastic_compare('>')  -> gt;
elastic_compare('>=') -> gte; 
elastic_compare('<')  -> lt;
elastic_compare('=<') -> lte.


%%
elastic_geo_distance(ElasticKey, {GeoHash, Radius}) ->
   #{geo_distance => #{distance => Radius, ElasticKey => GeoHash}};
elastic_geo_distance(ElasticKey, {Lng, Lat, Radius}) ->
   #{geo_distance => #{distance => Radius, ElasticKey => [Lng, Lat]}};
elastic_geo_distance(_, _) ->
   [].

%%
elastic_geo_shape(ElasticKey, {Index, Id, Field}) ->
   #{geo_shape => #{
      ElasticKey => #{
         indexed_shape => #{
            index => Index 
         ,  type => <<"_doc">>
         ,  id => Id
         ,  path => Field
         } 
      }
   }};

elastic_geo_shape(ElasticKey, Polygon) ->
   #{geo_shape => #{
      ElasticKey => #{
         shape => #{
            type => <<"Polygon">>
         ,  coordinates => [polygon(Polygon)]
         }
      }
   }}.

polygon([Lng, Lat | Tail]) ->
   [[Lng, Lat] | polygon(Tail)];
polygon([]) ->
   [].

%%
%%
aggregate({histogram, Bin, Key}) ->
   {bucket, Key,
      #{
         Key => #{
            histogram => #{
               field    => Key,
               interval => Bin
            }
         }
      }
   };

aggregate({histogram, Min, Max, Bin, Key}) ->
   {bucket, Key,
      #{
         Key => #{
            histogram => #{
               field    => Key,
               interval => Bin,
               extended_bounds => #{
                  min => Min,
                  max => Max
               }
            }
         }
      }
   };

aggregate({category, N, Key}) ->
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

aggregate({category, Key}) ->
   {bucket, Key,
      #{
         Key => #{
            terms => #{
               field => Key
            }
         }
      }
   };

aggregate({geohash, Bin, Key}) ->
   {bucket, Key,
      #{
         Key => #{
            geohash_grid => #{
               field     => Key,
               precision => Bin
            }
         }
      }
   };

aggregate({stats, Key}) ->
   {object, Key,
      #{
         Key => #{
            extended_stats => #{
               field => Key
            }
         }
      }
   };

aggregate({stats, Stats, Key}) ->
   {object, {Key, Stats},
      #{
         Key => #{
            extended_stats => #{
               field => Key
            }
         }
      }
   };

aggregate({percentiles, Key}) ->
   {object, Key,
      #{
         Key => #{
            percentiles => #{
               field => Key
            }
         }
      }
   };

aggregate({percentiles, Nth, Key}) ->
   {object, {Key, Nth},
      #{
         Key => #{
            percentiles => #{
               field => Key
            }
         }
      }
   };

aggregate({geo_bounds, Key}) ->
   {object, Key,
      #{
         Key => #{
            geo_bounds => #{
               field => Key
            }
         }
      }
   };

aggregate({geo_centroid, Key}) ->
   {object, Key,
      #{
         Key => #{
            geo_centroid => #{
               field => Key
            }
         }
      }
   };

aggregate({sum, Key}) ->
   {metric, Key,
      #{
         Key => #{
            sum => #{
               field => Key
            }
         }
      }
   };

aggregate({count, Key}) ->
   {metric, Key,
      #{
         Key => #{
            value_count => #{
               field => Key
            }
         }
      }
   };

aggregate(Key) ->
   {identity, Key, undefined}.

