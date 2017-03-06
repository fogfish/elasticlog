%%
%%   Copyright 2016 - 2017 Dmitry Kolesnikov, All Rights Reserved
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
-module(elasticlog_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile({parse_transform, category}).

%%
%% common test
-export([
   all/0
  ,groups/0
  ,init_per_suite/1
  ,end_per_suite/1
  ,init_per_group/2
  ,end_per_group/2
]).

-export([
   lookup_by_rdf_id/1,
   lookup_by_lang_string/1
]).

%%
%% docker run -it -p 9200:9200 elasticsearch
-define(ELASTIC, "http://localhost:9200/elasticlog").

%%%----------------------------------------------------------------------------   
%%%
%%% suite
%%%
%%%----------------------------------------------------------------------------   
all() ->
   [
      {group, interface}
   ].

groups() ->
   [
      {interface, [parallel], 
         [lookup_by_rdf_id, lookup_by_lang_string]}
   ].

%%%----------------------------------------------------------------------------   
%%%
%%% init
%%%
%%%----------------------------------------------------------------------------   
init_per_suite(Config) ->
   {ok, _} = application:ensure_all_started(elasticlog),
   ok = define_semantic(Config),
   ok = create_database(Config),
   ok = upload_database(Config),
   Config.

end_per_suite(_Config) ->
   ok.

%% 
%%
init_per_group(_, Config) ->
   Config.

end_per_group(_, _Config) ->
   ok.

%%%----------------------------------------------------------------------------   
%%%
%%% unit tests
%%%
%%%----------------------------------------------------------------------------   

%%
lookup_by_rdf_id(_Config) ->
   #{
      '@type'     := test,
      'rdf:id'    := <<"person:149">>,
      'foaf:name' := <<"Sophie Marceau">>
   } = eval("test(rdf:id, foaf:name) :- foaf:person(rdf:id, foaf:name, _, _), rdf:id = \"person:149\" . ").

%%
lookup_by_lang_string(_Config) ->
   #{
      '@type'     := test,
      'rdf:id'    := <<"person:137">>,
      'foaf:name' := <<"Ridley Scott">>,
      'foaf:birthday' := <<"1937-11-30">>
   } = eval("test(rdf:id, foaf:name, foaf:birthday) :- foaf:person(rdf:id, foaf:name, foaf:birthday, _), foaf:name = \"Ridley Scott\" . ").



%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
define_semantic(_Config) ->
   semantic:create(semantic:p("rdf:id", "xsd:string")),  %% @todo: reference

   semantic:create(semantic:p("foaf:name", "rdf:langString")),
   semantic:create(semantic:p("foaf:birthday", "xsd:string")),
   semantic:create(semantic:p("foaf:deathday", "xsd:string")),
   semantic:create(semantic:seq("foaf:person", 
      ["rdf:id", "foaf:name", "foaf:birthday", "foaf:deathday"])),

   semantic:create(semantic:p("dc:title", "rdf:langString")),
   semantic:create(semantic:p("imdb:year", "xsd:integer")),
   semantic:create(semantic:p("imdb:director", "xsd:string")), %% @todo: reference
   semantic:create(semantic:p("imdb:cast", "xsd:string")), %% @todo: reference
   semantic:create(semantic:p("imdb:sequel", "xsd:string")), %% @todo: reference
   semantic:create(semantic:seq("imdb:movie", 
      ["rdf:id", "dc:title", "imdb:year", "imdb:director", "imdb:cast", "imdb:sequel"])),
   ok.

%%
%%
create_database(Config) ->
   {ok, Sock} = esio:socket(?ELASTIC),
   esio:put(Sock, undefined, elasticlog:schema(["foaf:person", "imdb:movie"])),
   ok.


%%
%%
upload_database(Config) ->
   [$^||
      filename(Config),
      file:read_file(_),
      jsonify(_),
      publish_to_elastic(_)
   ].

filename(Config) ->
   {ok, filename:join([?config(data_dir, Config), "imdb.json"])}.

jsonify(X) ->
   {ok, jsx:decode(X)}.

publish_to_elastic(Json) ->
   {ok, Sock} = esio:socket(?ELASTIC),
   lists:foreach(
      fun(X) ->
         Id   = lens:get(lens:pair(<<"rdf:id">>), X),
         Type = lens:get(lens:pair(<<"rdf:type">>), X),
         {ok, _} = esio:put(Sock, {Type, Id}, maps:from_list(X))
      end,
      Json
   ).

%%
%%
eval(Datalog) ->
   {ok, Sock} = esio:socket(?ELASTIC),
   Query  = elasticlog:c(Datalog),
   Stream = (Query(#{}))(Sock),
   esio:close(Sock),
   stream:head(Stream).

