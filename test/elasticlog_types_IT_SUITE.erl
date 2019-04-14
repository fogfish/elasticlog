%% @doc
%%   integration test suite (requires elastic at localhost)
%%
%%   docker run -p 9200:9200 -d fogfish/elasticsearch:6.2.3
%%
-module(elasticlog_types_IT_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
   xsd_anyuri/1
,  xsd_string/1
,  xsd_integer/1
,  xsd_decimal/1
,  xsd_boolean/1
,  xsd_dateTime/1
,  xsd_date/1
,  xsd_time/1
,  xsd_yearmonth/1
,  xsd_monthday/1
,  xsd_year/1
,  xsd_month/1
,  xsd_day/1
,  georss_point/1
,  georss_hash/1
,  georss_json/1
,  undefined/1
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
schema(_Config) ->
   {ok, Sock} = esio:socket("http://localhost:9200/datatypes"),

   ok = elasticlog:schema(Sock, #{
      <<"xsd:anyURI">> => <<"xsd:anyURI">>,
      <<"xsd:string">> => <<"xsd:string">>,
      <<"xsd:integer">> => <<"xsd:integer">>,
      <<"xsd:decimal">> => <<"xsd:decimal">>,
      <<"xsd:boolean">> => <<"xsd:boolean">>,
      <<"xsd:dateTime">> => <<"xsd:dateTime">>,
      <<"xsd:date">> => <<"xsd:date">>,
      <<"xsd:time">> => <<"xsd:time">>,
      <<"xsd:gYearMonth">> => <<"xsd:gYearMonth">>,
      <<"xsd:gMonthDay">> => <<"xsd:gMonthDay">>,
      <<"xsd:gYear">> => <<"xsd:gYear">>,
      <<"xsd:gMonth">> => <<"xsd:gMonth">>,
      <<"xsd:gDay">> => <<"xsd:gDay">>,
      <<"georss:point">> => <<"georss:point">>,
      <<"georss:hash">> => <<"georss:hash">>,
      <<"georss:json">> => <<"georss:json">>,
      <<"undefined">> => <<"xsd:anyURI">>
   }),

   esio:close(Sock).

intake(Config) ->
   {ok, Sock} = esio:socket("http://localhost:9200/datatypes"),
   {ok, File} = file:read_file(filename:join([?config(data_dir, Config), "datatypes.json"])),

   lists:foreach(
      fun(X) ->
         Id   = lens:get(lens:pair(<<"rdf:id">>), X),
         {ok, _} = esio:put(Sock, Id, maps:from_list(X))
      end,
      jsx:decode(File)
   ),

   esio:close(Sock).


xsd_anyuri(Config) ->
   F = datalog("
      ?- datatypes(xsd:anyURI, _).

      datatypes(\"rdf:id\", \"xsd:anyURI\").
   "),

   [
      [_, {iri, <<"http://example.com/a">>}]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


xsd_string(Config) ->
   F = datalog("
      ?- datatypes(xsd:string, _).

      datatypes(\"rdf:id\", \"xsd:string\").
   "),

   [
      [_, <<"example">>]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


xsd_integer(Config) ->
   F = datalog("
      ?- datatypes(xsd:integer, _).

      datatypes(\"rdf:id\", \"xsd:integer\").
   "),

   [
      [_, 123]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


xsd_decimal(Config) ->
   F = datalog("
      ?- datatypes(xsd:decimal, _).

      datatypes(\"rdf:id\", \"xsd:decimal\").
   "),

   [
      [_, 12.3]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


xsd_boolean(Config) ->
   F = datalog("
      ?- datatypes(xsd:boolean, _).

      datatypes(\"rdf:id\", \"xsd:boolean\").
   "),

   [
      [_, true]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


xsd_dateTime(Config) ->
   F = datalog("
      ?- datatypes(xsd:dateTime, _).

      datatypes(\"rdf:id\", \"xsd:dateTime\").
   "),

   [
      [_, {997,265730,0}]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


xsd_date(Config) ->
   F = datalog("
      ?- datatypes(xsd:date, _).

      datatypes(\"rdf:id\", \"xsd:date\").
   "),

   [
      [_, {{2001, 8, 8}, {0, 0, 0}}]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


xsd_time(Config) ->
   F = datalog("
      ?- datatypes(xsd:time, _).

      datatypes(\"rdf:id\", \"xsd:time\").
   "),

   [
      [_, {{0, 0, 0}, {10, 15, 30}}]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


xsd_yearmonth(Config) ->
   F = datalog("
      ?- datatypes(xsd:gYearMonth, _).

      datatypes(\"rdf:id\", \"xsd:gYearMonth\").
   "),

   [
      [_, {{2001, 8, 0}, {0, 0, 0}}]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


xsd_monthday(Config) ->
   F = datalog("
      ?- datatypes(xsd:gMonthDay, _).

      datatypes(\"rdf:id\", \"xsd:gMonthDay\").
   "),

   [
      [_, {{0, 8, 8}, {0, 0, 0}}]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


xsd_year(Config) ->
   F = datalog("
      ?- datatypes(xsd:gYear, _).

      datatypes(\"rdf:id\", \"xsd:gYear\").
   "),

   [
      [_, {{2001, 0, 0}, {0, 0, 0}}]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


xsd_month(Config) ->
   F = datalog("
      ?- datatypes(xsd:gMonth, _).

      datatypes(\"rdf:id\", \"xsd:gMonth\").
   "),

   [
      [_, {{0, 8, 0}, {0, 0, 0}}]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


xsd_day(Config) ->
   F = datalog("
      ?- datatypes(xsd:gDay, _).

      datatypes(\"rdf:id\", \"xsd:gDay\").
   "),

   [
      [_, {{0, 0, 8}, {0, 0, 0}}]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).

georss_point(Config) ->
   F = datalog("
      ?- datatypes(georss:point, _).

      datatypes(\"rdf:id\", \"georss:point\").
   "),

   [
      [_, {60.1, 23.2}]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


georss_hash(Config) ->
   F = datalog("
      ?- datatypes(georss:hash, _).

      datatypes(\"rdf:id\", \"georss:hash\").
   "),

   [
      [_, <<"ueh6xcb">>]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).


georss_json(Config) ->
   F = datalog("
      ?- datatypes(georss:json, _).

      datatypes(\"rdf:id\", \"georss:json\").
   "),

   [
      [_, #{<<"type">> := <<"Point">>, <<"coordinates">> := [23.2, 60.1]}]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).

undefined(Config) ->
   F = datalog("
      ?- datatypes(\"undefined\", _).

      datatypes(\"rdf:id\", option \"undefined\").
   "),

   [
      [_, undefined]
   ] = stream:list(elasticlog:q(F, ?config(socket, Config))).
