%% @doc
%%   integration test suite (requires elasticlog at localhost)
%%
%%   docker run -p 9200:9200 -d fogfish/elasticsearch:6.2.3
%%
-module(elasticlog_IT_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
   basic_facts_query/1
,  basic_facts_query_with_field_spec/1
,  basic_facts_match/1
,  basic_facts_infix/1
,  join_facts_with_bag/1
,  join_facts/1
,  rollup_facts/1
,  rollup_facts_and_join/1
,  implicit/1
,  equivalent/1
,  equivalent_with_field_spec/1
]).

all() -> 
   [Test || {Test, NAry} <- ?MODULE:module_info(exports), 
      Test =/= module_info,
      Test =/= init_per_suite,
      Test =/= end_per_suite,
      NAry =:= 1
   ].

init_per_suite(Config) ->
   {ok, _} = elasticlog:start(),
   {ok, Sock} = esio:socket("http://localhost:9200/*"),
   schema(Config),
   intake(Config),
   %% let's wait when index is refreshed
   timer:sleep(5000),
   [{socket, Sock} | Config].

end_per_suite(Config) ->
   esio:close(?config(socket, Config)),
   ok.

datalog(Datalog) ->
   datalog:c(elasticlog, datalog:p(Datalog)).

%%
%% Unit tests
%%
schema(_) ->
   {ok, Sock} = esio:socket("http://localhost:9200/imdb"),

   ok = elasticlog:schema(Sock, #{
      <<"schema:name">> => <<"xsd:string">>,
      <<"schema:born">> => <<"xsd:date">>,
      <<"schema:death">> => <<"xsd:date">>,
      <<"schema:title">> => <<"xsd:string">>,
      <<"schema:year">> => <<"xsd:integer">>,
      <<"schema:director">> => <<"xsd:anyURI">>,
      <<"schema:cast">> => <<"xsd:anyURI">>,
      <<"schema:sequel">> => <<"xsd:anyURI">>
   }),

   esio:close(Sock).

%%
%%
intake(_) ->
   {ok, Sock} = esio:socket("http://localhost:9200/imdb"),

   Stream = semantic:nt(filename:join([code:priv_dir(datalog), "imdb.nt"])),
   stream:foreach(
      fun(Fact)-> 
         {ok, _} = elasticlog:append(Sock, Fact) 
      end,
      semantic:fold(
         stream:map(fun semantic:typed/1, Stream)
      )
   ),

   esio:close(Sock).

%%
%%
basic_facts_query(Config) ->
   F = datalog("
      ?- imdb:person(_, \"Ridley Scott\").

      imdb:person(\"rdf:id\", \"schema:name\").
   "),

   [
      [{iri,<<"http://example.org/person/137">>}, <<"Ridley Scott">>]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).

%%
%%
basic_facts_query_with_field_spec(Config) ->
   F = datalog("
      ?- imdb:person(_, \"Ridley Scott\", _).

      imdb:person(\"rdf:id\", required \"schema:name\", option \"schema:death\").
   "),

   [
      [{iri,<<"http://example.org/person/137">>}, <<"Ridley Scott">>, undefined]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


%%
%%
basic_facts_match(Config) ->
   F = datalog("
      ?- movie(_, _). 

      imdb:movie(sortby \"rdf:id\", \"schema:title\", \"schema:year\").

      movie(id, title) :- 
         imdb:movie(id, title, 1987).
   "),

   [
      [{iri,<<"http://example.org/movie/202">>}, <<"Predator">>],
      [{iri,<<"http://example.org/movie/203">>}, <<"Lethal Weapon">>],
      [{iri,<<"http://example.org/movie/204">>}, <<"RoboCop">>]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).

%%
%%
basic_facts_infix(Config) ->
   F = datalog("
      ?- movie(_, _).

      imdb:movie(sortby \"rdf:id\", \"schema:title\", \"schema:year\").

      movie(title, year) :- 
         imdb:movie(id, title, year), year < 1984.
   "),

   [
      [<<"First Blood">>, 1982],
      [<<"Alien">>, 1979],
      [<<"Mad Max">>, 1979],
      [<<"Mad Max 2">>, 1981]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


%%
%%
join_facts_with_bag(Config) ->
   F = datalog("
      ?- actors(_, _).

      imdb:movie(sortby \"rdf:id\", \"schema:title\", \"schema:year\", \"schema:cast\").
      imdb:person(\"rdf:id\", \"schema:name\").

      actors(title, name) :- 
         imdb:movie(id, title, year, cast), 
         .flat(cast), 
         imdb:person(cast, name), 
         year < 1984.
   "),

   [
      [<<"First Blood">>,<<"Sylvester Stallone">>],
      [<<"First Blood">>,<<"Richard Crenna">>],
      [<<"First Blood">>,<<"Brian Dennehy">>],
      [<<"Alien">>,<<"Tom Skerritt">>],
      [<<"Alien">>,<<"Sigourney Weaver">>],
      [<<"Alien">>,<<"Veronica Cartwright">>],
      [<<"Mad Max">>,<<"Mel Gibson">>],
      [<<"Mad Max">>,<<"Steve Bisley">>],
      [<<"Mad Max">>,<<"Joanne Samuel">>],
      [<<"Mad Max 2">>,<<"Mel Gibson">>],
      [<<"Mad Max 2">>,<<"Michael Preston">>],
      [<<"Mad Max 2">>,<<"Bruce Spence">>]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


%%
%%
join_facts(Config) ->
   F = datalog("
      ?- movie(_).

      imdb:movie(sortby \"rdf:id\", \"schema:title\", \"schema:cast\").
      imdb:person(\"rdf:id\", \"schema:name\").

      movie(title) :- 
         imdb:person(id, name), 
         imdb:movie(movie, title, id), 
         name = \"Sylvester Stallone\".
   "),

   [
      [<<"First Blood">>],
      [<<"Rambo: First Blood Part II">>],
      [<<"Rambo III">>]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


%%
%%
rollup_facts(Config) ->
   F = datalog("
      ?- imdb:year(_).

      imdb:year(histogram 10 \"schema:year\").
   "),

   [
      [#{<<"count">> :=  2, <<"key">> := 1970.0}],
      [#{<<"count">> := 13, <<"key">> := 1980.0}],
      [#{<<"count">> :=  4, <<"key">> := 1990.0}],
      [#{<<"count">> :=  1, <<"key">> := 2000.0}]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).

%%
%%
rollup_facts_and_join(Config) ->
   F = datalog("
      ?- top(_, _).

      imdb:director(category 5 \"schema:director\").
      imdb:person(\"rdf:id\", \"schema:name\").

      top(name, id) :- 
         imdb:director(id),
         imdb:person(id, name).
   "),

   [
      [<<"James Cameron">>, #{<<"count">> := 3, <<"key">> := <<"http://example.org/person/100">>}],
      [<<"Richard Donner">>,#{<<"count">> := 3, <<"key">> := <<"http://example.org/person/111">>}],
      [<<"George Miller">>, #{<<"count">> := 3, <<"key">> := <<"http://example.org/person/142">>}],
      [<<"John McTiernan">>,#{<<"count">> := 2, <<"key">> := <<"http://example.org/person/108">>}],
      [<<"Ted Kotcheff">>,  #{<<"count">> := 1, <<"key">> := <<"http://example.org/person/104">>}]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).

%%
%%
implicit(Config) ->
   F = datalog("
      ?- imdb:person(_, _).

      imdb:person(\"rdf:id\", \"schema:name\").
   "),

   [
      [{iri,<<"http://example.org/person/137">>}, <<"Ridley Scott">>]
   ] = stream:list(elasticlog:q(F, #{<<"schema:name">> => <<"Ridley Scott">>}, ?config(socket, Config))).

%%
%%
equivalent(Config) ->
   F = datalog("
      ?- imdb:person(_, \"Ridley Scott\").

      imdb:person(\"rdf:id\", \"foaf:name\").
   "),

   [
      [{iri,<<"http://example.org/person/137">>}, <<"Ridley Scott">>]
   ] = stream:list(elasticlog:q(F, undefined, #{<<"foaf:name">> => <<"schema:name">>}, ?config(socket, Config))).

%%
%%
equivalent_with_field_spec(Config) ->
   F = datalog("
      ?- imdb:person(_, \"Ridley Scott\").

      imdb:person(\"rdf:id\", required \"foaf:name\").
   "),

   [
      [{iri,<<"http://example.org/person/137">>}, <<"Ridley Scott">>]
   ] = stream:list(elasticlog:q(F, undefined, #{<<"foaf:name">> => <<"schema:name">>}, ?config(socket, Config))).
