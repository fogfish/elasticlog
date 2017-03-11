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
   rdf_id/1,
   rdf_lang_string/1,
   imdb_actor_of/1
]).

%%
%% docker run -it -p 9200:9200 elasticsearch
% -define(SERVICE, "http://localhost:9200").
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
         [rdf_id, rdf_lang_string, imdb_actor_of]}
   ].

%%%----------------------------------------------------------------------------   
%%%
%%% init
%%%
%%%----------------------------------------------------------------------------   
init_per_suite(Config) ->
   {ok, _} = application:ensure_all_started(elasticlog),
   lager:set_loglevel(lager_console_backend, debug),
   ok = define_semantic(Config),
   ok = create_database(Config),
   ok = upload_database(Config),
   %% let's wait when index is refreshed
   timer:sleep(5000),
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
rdf_id(_Config) ->
   [#{
      'rdf:id'    := <<"person:149">>,
      'foaf:name' := <<"Sophie Marceau">>
   }] = eval("test(rdf:id, foaf:name) :- foaf:person(rdf:id, foaf:name, _, _), rdf:id = \"person:149\" .").

%%
rdf_lang_string(_Config) ->
   [#{
      'rdf:id'    := <<"person:137">>,
      'foaf:name' := <<"Ridley Scott">>,
      'foaf:birthday' := <<"1937-11-30">>
   }] = eval("test(rdf:id, foaf:name, foaf:birthday) :- foaf:person(rdf:id, foaf:name, foaf:birthday, _), foaf:name = \"Ridley Scott\" .").


%%
%%
imdb_actor_of(_Config) ->
   %% note query matches: all Lethal Weapon sequels
   [
      #{'foaf:name' := <<"Mel Gibson">>},
      #{'foaf:name' := <<"Danny Glover">>},
      #{'foaf:name' := <<"Joe Pesci">>},
      #{'foaf:name' := <<"Gary Busey">>}
   ] = eval("rel(foaf:name) :- imdb:movie(_, dc:title, _, _, imdb:cast, _), .flat(imdb:cast), .unique(imdb:cast), foaf:person(imdb:cast, foaf:name, _, _), dc:title = \"Lethal Weapon\" .").

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
define_semantic(_Config) ->
   semantic:create(semantic:p("foaf:name", "rdf:langString")),
   semantic:create(semantic:p("foaf:birthday", "xsd:string")),
   semantic:create(semantic:p("foaf:deathday", "xsd:string")),
   semantic:create(semantic:seq("foaf:person", 
      ["rdf:id", "foaf:name", "foaf:birthday", "foaf:deathday"])),

   semantic:create(semantic:p("dc:title", "rdf:langString")),
   semantic:create(semantic:p("imdb:year", "xsd:integer")),
   semantic:create(semantic:p("imdb:director", "xsd:anyURI")),
   semantic:create(semantic:p("imdb:cast", "xsd:anyURI")),
   semantic:create(semantic:p("imdb:sequel", "xsd:anyURI")),
   semantic:create(semantic:seq("imdb:movie", 
      ["rdf:id", "dc:title", "imdb:year", "imdb:director", "imdb:cast", "imdb:sequel"])),
   ok.

%%
%%
create_database(Config) ->
   {ok, Sock} = esio:socket(?ELASTIC),
   esio:put(Sock, {urn, undefined, <<>>}, elasticlog:schema(["foaf:person", "imdb:movie"])),
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
         {ok, _} = esio:put(Sock, {urn, Type, Id}, maps:from_list(X))
      end,
      Json
   ).

%%
%%
eval(Datalog) ->
   {ok, Sock} = esio:socket(?ELASTIC),
   Query  = elasticlog:c(Datalog),
   Stream = stream:list(datalog:q(Query, Sock)),
   esio:close(Sock),
   Stream.

