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

-include("elasticlog.hrl").
-include_lib("datum/include/datum.hrl").

-export([start/0]).
-export([
   c/1,
   q/2,
   q/3,
   q/4,
   schema/1,
   schema/2,
   schema/3,
   append/2,
   append/3,
   append_/2,
   append_/3,
   stream/3,
   encode/1,
   decode/1,
   jsonify/1,
   identity/1
]).

%%
%%
-type sock()   :: _.
-type schema() :: #{attr() => type()}.
-type attr()   :: semantic:uri().
-type type()   :: semantic:compact().

%%
%%
start() ->
   application:ensure_all_started(?MODULE).

%%
%% compiles datalog query
c(Datalog) ->
   datalog:c(?MODULE, datalog:p(Datalog), [{return, maps}]).

%%
%% evaluate query
q(Lp, Sock) ->
   Lp(#elasticlog{sock = Sock}).

q(Lp, Implicit, Sock) ->
   Lp(#elasticlog{implicit = Implicit, sock = Sock}).

q(Lp, Implicit, Equiv, Sock) ->
   Lp(#elasticlog{implicit = Implicit, equivalent = Equiv, sock = Sock}).

%%
%% read semantic schema
-spec schema(sock()) -> datum:either( schema() ).

schema(Sock) ->
   [either ||
      esio:schema(Sock),
      cats:unit(elasticlog_schema:predicate(_))
   ].

%%
%% build schema for semantic data
-spec schema(sock(), schema()) -> datum:either().
-spec schema(sock(), schema(), [_]) -> datum:either().

schema(Sock, Schema) ->
   schema(Sock, Schema, []).

schema(Sock, Schema, Opts) ->
   [either ||
      ElasticSchema <- cats:unit( elasticlog_schema:new(Schema, Opts) ),
      schema_deploy_bucket(Sock, ElasticSchema),
      schema_deploy_fields(Sock, schema_fields(ElasticSchema))
   ].

schema_deploy_bucket(Sock, Schema) ->
   case esio:schema(Sock, maps:with([settings], Schema)) of
      {ok, _} ->
         ok;
      {error, {400, #{<<"error">> := #{<<"type">> := <<"resource_already_exists_exception">>}}}} ->
         ok;
      {error, _} = Error ->
         Error
   end.

schema_deploy_fields(_, []) ->
   ok;
schema_deploy_fields(Sock, [Head | Tail]) ->
   case esio:schema(Sock, Head) of
      {ok, _} ->
         schema_deploy_fields(Sock, Tail);
      {error, _} = Error ->
         Error
   end.

schema_fields(Schema) ->
   [lens:put(lens_schema_properties(Key), Type, #{}) 
      || {Key, Type} <- maps:to_list(lens:get(lens_schema_properties(), Schema))].

lens_schema_properties() ->
   lens:c(lens:at(mappings, #{}), lens:at('_doc', #{}), lens:at(properties, #{})).

lens_schema_properties(Key) ->
   lens:c(lens:at(properties, #{}), lens:at(Key)).

%%
%% append knowledge fact 
-spec append(sock(), semantic:spo()) -> datum:either( semantic:iri() ).
-spec append(sock(), semantic:spo(), timeout()) -> datum:either( semantic:iri() ).

append(Sock, Fact) ->
   append(Sock, Fact, 30000).

append(Sock, Fact, Timeout) ->
   elasticlog_nt:append(Sock, Fact, Timeout).


-spec append_(sock(), semantic:spo()) -> ok | reference().
-spec append_(sock(), semantic:spo(), boolean()) -> ok | reference().

append_(Sock, Fact) ->
   append_(Sock, Fact, false).

append_(Sock, Fact, Flag) ->
   elasticlog_nt:append_(Sock, Fact, Flag).


%%
%% datalog generators
stream({iri, Bucket, _}, Keys, Head) ->
   case elasticlog_syntax:is_aggregate(Keys) of
      false ->
         elasticlog_q:stream(Bucket, Keys, Head);
      true  ->
         elasticlog_s:stream(Bucket, Keys, Head)
   end;

stream(Bucket, Keys, Head) when is_atom(Bucket) ->
   stream({iri, typecast:s(Bucket), undefined}, Keys, Head).

%%
%% encodes JSON-LD to storage format
-spec encode(_) -> _.

encode(#{<<"@id">> := Id} = Json) ->
   [identity ||
      maps:remove(<<"@id">>, Json),
      cats:unit(_#{<<"rdf:id">> => Id})
   ];

encode(#{<<"rdf:id">> := _} = Json) ->
   Json.
   
%%
%% decodes storage format to JSON-LD
-spec decode(_) -> _.

decode(#{<<"rdf:id">> := Id} = Json) ->
   [identity ||
      maps:remove(<<"rdf:id">>, Json),
      cats:unit(_#{<<"@id">> => Id})
   ];

decode(#{<<"@id">> := _} = Json) ->
   Json.

%%
%% encodes deducted fact(s) to json format
-spec jsonify(datum:stream()) -> datum:stream(). 

jsonify(Stream) ->
   stream:map(
      fun(Fact) ->
         maps:map(
            fun(_, Val) -> semantic:to_json(Val) end,
            maps:filter(
               fun(_, undefined) -> false; (_, _) -> true end,
               Fact
            )
         )
      end,
      Stream
   ).

%%
%% encode any string to unique knowledge identity
-spec identity(_) -> binary().

identity(Key) ->
   elasticlog_nt:identity(Key).
