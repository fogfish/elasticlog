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
-include_lib("datum/include/datum.hrl").

-export([start/0]).
-export([
   schema/1,
   schema/2,
   schema/3,
   append/2,
   append/3,
   append_/2,
   append_/3,
   stream/2,
   encode/1,
   decode/1,
   jsonify/2,
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
   esio:schema(Sock, elasticlog_schema:new(Schema, Opts)).


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
%% datalog stream generator
stream(Keys, Head) ->
   elasticlog_q:stream(Keys, Head).


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
jsonify(Schema, #stream{} = Stream) ->
   stream:map(
      fun(Fact) ->
         maps:from_list(lists:zip(Schema, [json_val(X) || X <- Fact]))
      end,
      Stream
   ).

json_val({iri, Uri}) -> 
   Uri;
json_val({iri, Prefix, Suffix}) -> 
   <<Prefix/binary, $:, Suffix/binary>>;
json_val({_, _, _} = T) -> 
   scalar:s(tempus:encode(T));
json_val(Value) when is_atom(Value) -> 
   scalar:s(Value);
json_val(Value) when is_float(Value) -> 
   Value;
json_val(Value) when is_integer(Value) -> 
   Value;
json_val(Value) ->
   scalar:s(Value).

%%
%% encode any string to unique knowledge identity
identity(Key) ->
   elasticlog_nt:identity(Key).

