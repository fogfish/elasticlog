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

-export([stream/3]).

stream(Bucket, Keys, Head) ->
   fun(#elasticlog{implicit = Implicit, equivalent = Equiv, sock = Sock}) ->
      [identity ||
         ElasticKeys <- elasticlog_syntax:keys(Keys, Equiv),
         schema(Sock, Bucket, ElasticKeys),
         Schema <- lists:zip3(_, ElasticKeys, Head),
         elasticlog_syntax:pattern(Schema, Implicit),
         enable_sorting(hd(ElasticKeys), _),
         log_elastic_query(_),
         esio:stream(Sock, Bucket, _),
         head(Schema, _)
      ]
   end.

schema(Sock, Bucket, Keys) ->
   Schema = elasticlog_schema:lookup(Sock, Bucket),
   [maps:get(Key, Schema) || {_, Key} <- Keys].

head(Schema, Stream) ->
   stream:map(
      fun(#{<<"_source">> := Json, <<"_score">> := _Score}) ->
         lists:map(
            fun({Type, {_, Key}, _}) ->
               Path = binary:split(Key, <<$.>>, [global]),
               Lens = lens:c([lens:at(X) || X <- Path]),
               [option || lens:get(Lens, Json), semantic:as_text(Type, _)]
            end,
            Schema
         )
      end,
      Stream
   ).

enable_sorting({_, Key}, Query) ->
   Query#{sort => Key};
enable_sorting(Key, Query) ->
   Query#{sort => Key}.

log_elastic_query(Query) ->
   error_logger:info_msg("~s~n", [jsx:encode(Query)]),
   Query.

