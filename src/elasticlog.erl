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
%%   datalog interface for elastic search
-module(elasticlog).

-compile({parse_transform, category}).

-export([start/0]).
-export([
   schema/1,
   schema/2,
   schema/3,
   append/2,
   append/3,

   p/1,
   c/1,
   horn/2,
   encode/1,
   decode/1
]).

%%
%%
-type sock() :: _.

%%
%%
start() ->
   application:ensure_all_started(?MODULE).

%%
%% read semantic schema
-spec schema(sock()) -> datum:either(_).

schema(Sock) ->
   [either ||
      esio:schema(Sock),
      cats:unit(elasticlog_schema:predicate(_))
   ].

%%
%% build schema for semantic data
-spec schema(sock(), _) -> datum:either().
-spec schema(sock(), _, [_]) -> datum:either().

schema(Sock, Schema) ->
   schema(Sock, Schema, []).

schema(Sock, Schema, Opts) ->
   esio:schema(Sock, elasticlog_schema:new(Schema, Opts)).


%%
%% append knowledge fact 
-spec append(sock(), semantic:spo()) -> datum:either( semantic:iri() ).
-spec append(sock(), semantic:spo(), timeout()) -> datum:either( semantic:iri() ).

append(Sock, Fact) ->
   append(Sock, Fact, 30000).

append(Sock, Fact, Timeout) ->
   elasticlog_nt:append(Sock, Fact, Timeout).

%%
%% parse datalog query
p(Datalog) ->
   datalog:pflat(Datalog).

%%
%% compile native query
c(Datalog) ->
   datalog:cflat(elasticlog_q, Datalog).


%%
%% declare horn clause using native query syntax
%%  Example:
%%    datalog:q(
%%       #{x => ...},     % define query goal
%%       elasticlog:horn([x, y], [
%%          #{'@' => ..., '_' => [x,y,z], z => ...}
%%       ])
%%    ).
horn(Head, List) ->
   datalog:c(elasticlog_q, #{q => [Head | List]}).

%%
%% 
-spec encode(_) -> _.

encode(#{<<"@id">> := Id} = Json) ->
   [identity ||
      maps:remove(<<"@id">>, Json),
      cats:unit(_#{<<"rdf:id">> => Id})
   ];

encode(#{<<"rdf:id">> := _} = Json) ->
   Json.
   
%%
%%
-spec decode(_) -> _.

decode(#{<<"rdf:id">> := Id} = Json) ->
   [identity ||
      maps:remove(<<"rdf:id">>, Json),
      cats:unit(_#{<<"@id">> => Id})
   ];

decode(#{<<"@id">> := _} = Json) ->
   Json.

