%% @doc
%%   datalog sigma function, advanced streaming with aggregation 
-module(elasticlog_s).
-compile({parse_transform, category}).
-include("elasticlog.hrl").

-export([stream/3]).

stream(Bucket, Keys, Head) ->
   fun(#elasticlog{implicit = Implicit, equivalent = Equiv, sock = Sock}) ->
      [identity ||
         ElasticKeys <- elasticlog_syntax:keys(Keys, Equiv),
         schema(Sock, Bucket, ElasticKeys),
         Schema <- lists:zip3(_, ElasticKeys, Head),
         Aggs <- lists:map(fun elasticlog_syntax:aggregate/1, Keys),
         q(Schema, Implicit, Aggs),
         log_elastic_query(_),
         esio:lookup(Sock, Bucket, _, 10000),
         stream(_, Aggs)
      ]
   end.

schema(Sock, Bucket, Keys) ->
   Schema = elasticlog_schema:lookup(Sock, Bucket),
   [maps:get(Key, Schema) || {_, Key} <- Keys].

%%
%%
q(Pattern, Implicit, Aggs) ->
   BaseQuery = elasticlog_syntax:pattern(Pattern, Implicit),
   SubQuery = lists:foldr(fun fold/2, #{}, Aggs),
   BaseQuery#{
      aggs => SubQuery,
      size => 0
   }.

fold({bucket, _, Spec}, SubQuery) when map_size(SubQuery) =:= 0 ->
   Spec;
fold({bucket, Key, Spec}, SubQuery) ->
   lens:put(lens:c(lens:at(Key), lens:at(aggs)), SubQuery, Spec);
fold({object, _, Spec}, SubQuery) ->
   maps:merge(Spec, SubQuery);
fold({metric, _, Spec}, SubQuery) ->
   maps:merge(Spec, SubQuery);
fold({identity, _, _}, SubQuery) ->
   SubQuery.

%%
%%
stream({ok, #{<<"aggregations">> := Json}}, Aggs) ->
   stream:build(lists:reverse(decode(Aggs, Json, [], []))).

%%
decode([Head | Tail], Aggs, Prefix, Acc) ->
   case downfield(Head, Aggs) of
      X when is_list(X) ->
         lists:foldl(
            fun(Xx, Acc0) -> decode_bucket(Tail, Xx, Prefix, Acc0) end, Acc, X
         );
      X ->
         decode(Tail, Aggs, [X | Prefix], Acc)
   end;

decode([], _, Prefix, Acc) ->
   [lists:reverse(Prefix) | Acc].


%%
decode_bucket(Tail, #{<<"doc_count">> := Count, <<"key">> := Key} = Bucket, Prefix, Acc) ->
   decode(Tail, Bucket, [#{<<"key">> => Key, <<"count">> => Count} | Prefix], Acc).

%%
downfield({bucket, [Key, SubKey], _}, Aggs) ->
   lens:get(lens:c(lens:at(Key), lens:at(<<"buckets">>), lens:traverse(), lens:at(SubKey)), Aggs);

downfield({bucket, Key, _}, Aggs) ->
   lens:get(lens:c(lens:at(Key), lens:at(<<"buckets">>)), Aggs);

downfield({metric, Key, _}, Aggs) ->
   lens:get(lens:c(lens:at(Key), lens:at(<<"value">>)), Aggs);

downfield({object, Keys, _}, Aggs) when is_list(Keys) ->
   lens:get(lens(Keys), Aggs);

downfield({object, Key, _}, Aggs) ->
   lens:get(lens:at(Key), Aggs);

downfield({identity, _, _}, _) ->
   %% identity aggregation takes value from previous predicate.
   undefined.

%%
lens([Key]) ->
   lens:at(Key);
lens(Keys) ->
   lens:c(lens_nested(Keys)).

lens_nested([Key]) ->
   [lens:at(Key)];
lens_nested([Key | Keys]) ->
   [lens:at(Key, #{}) | lens_nested(Keys)].

log_elastic_query(Query) ->
   error_logger:info_msg("~s~n", [jsx:encode(Query)]),
   Query.
