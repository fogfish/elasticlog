-module(elasticlog_datatype_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("semantic/include/semantic.hrl").

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
   xsd_any_uri/1,
   xsd_string/1,
   rdf_lang_string/1,
   xsd_integer/1,
   xsd_decimal/1,
   xsd_datetime/1,
   georss_hash/1
]).


-define(ELASTIC, "http://localhost:9200/datatype").

%%%----------------------------------------------------------------------------   
%%%
%%% suite
%%%
%%%----------------------------------------------------------------------------   
all() ->
   [
      {group, datatype}
   ].

groups() ->
   [
      {datatype, [parallel], 
         [
            xsd_any_uri,
            xsd_string,
            rdf_lang_string,
            xsd_integer,
            xsd_decimal,
            xsd_datetime,
            georss_hash
         ]
      }
   ].

%%%----------------------------------------------------------------------------   
%%%
%%% init
%%%
%%%----------------------------------------------------------------------------   
init_per_suite(Config) ->
   elasticlog:start(),
   {ok, Sock} = esio:socket(?ELASTIC),
   esio:put(Sock, {urn, undefined, <<>>}, #{}),
   esio:close(Sock),
   Config.

end_per_suite(_Config) ->
   {ok, Sock} = esio:socket(?ELASTIC),
   esio:remove(Sock, {urn, undefined, <<>>}),
   esio:close(Sock),
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

xsd_any_uri(_Config) ->
   Type = <<"xsd:anyURI">>,
   Key  = 'dc:subject',
   Val  = <<"http://example.org/a/b/c">>,
   Log  = "test(rdf:id, dc:subject) :- xsd:anyURI(rdf:id, dc:subject), dc:subject = \"http://example.org/a/b/c\" .",
   #{Key := Val} = datatype(Type, Key, Val, Log).


xsd_string(_Config) ->
   Type = <<"xsd:string">>,
   Key  = 'dc:title',
   Val  = <<"xsd-string">>,
   Log  = "test(rdf:id, dc:title) :- xsd:string(rdf:id, dc:title), dc:title = \"xsd-string\" .",
   #{Key := Val} = datatype(Type, Key, Val, Log).

rdf_lang_string(_Config) ->
   Type = <<"rdf:langString">>,
   Key  = 'foaf:name',
   Val  = <<"rdf lang string">>,
   Log  = "test(rdf:id, foaf:name) :- rdf:langString(rdf:id, foaf:name), foaf:name = \"rdf\" .",
   #{Key := Val} = datatype(Type, Key, Val, Log).

xsd_integer(_Config) ->
   Type = <<"xsd:integer">>,
   Key  = 'foaf:age',
   Val  = 10,
   Log  = "test(rdf:id, foaf:age) :- xsd:integer(rdf:id, foaf:age), foaf:age = 10 .",
   #{Key := Val} = datatype(Type, Key, Val, Log).

%% @todo: xsd long, int, short, byte

xsd_decimal(_Config) ->
   Type = <<"xsd:decimal">>,
   Key  = 'foaf:rank',
   Val  = 10.0,
   Log  = "test(rdf:id, foaf:rank) :- xsd:decimal(rdf:id, foaf:rank), foaf:rank = 10.0 .",
   #{Key := Val} = datatype(Type, Key, Val, Log).


xsd_datetime(_Config) ->
   Type = <<"xsd:datetime">>,
   Key  = 'foaf:birthday',
   Val  = <<"20140301T010203Z">>,
   Log  = "test(rdf:id, foaf:birthday) :- xsd:datetime(rdf:id, foaf:birthday), foaf:birthday = \"20140301T010203Z\" .",
   #{Key := Val} = datatype(Type, Key, Val, Log).

georss_hash(_Config) ->
   Type = <<"georss:hash">>,
   Key  = 'georss:point',
   Val  = <<"drjk0xegcw06">>,
   Log  = "test(rdf:id, georss:point) :- georss:hash(rdf:id, georss:point), georss:point = (\"drn5x1g8cu2y\", \"120km\") .",
   #{Key := Val} = datatype(Type, Key, Val, Log).



%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
datatype(Type, Key, Value, Query) ->
   {ok, Sock} = esio:socket(?ELASTIC),

   %% define schema for given data type
   semantic:create(semantic:p(Key, Type)),
   semantic:create(semantic:seq(Type, Type, [?RDF_ID, Key])),
   ok = esio:put(Sock, {urn, <<"_mappings">>, Type}, elasticlog:schema([Type], [type])),

   %% define test data
   Uid = {urn, Type, <<"datatype">>},
   Val = #{'rdf:id' => <<"datatype">>, Key => Value},
   {ok, _} = esio:put(Sock, Uid, Val),
   {ok, _} = esio:get(Sock, Uid), 

   %% request test data
   Result = stream:head(datalog:q(elasticlog:c(Query), Sock)),
   
   esio:close(Sock),
   Result.


%%
%%



