# elasticlog

[Datalog](https://en.wikipedia.org/wiki/Datalog) query support for Elastic Search using the [parser](https://github.com/fogfish/datalog) implemented on Erlang.

[![Build Status](https://secure.travis-ci.org/fogfish/elasticlog.svg?branch=master)](http://travis-ci.org/fogfish/elasticlog)


## Inspiration 

Declarative logic programs is an alternative approach to extract knowledge facts in deductive databases. It is designed for applications that uses a large number of ground facts persisted in external storage. The library implements experimental support of datalog language to query knowledge facts stored in Elastic Search.


## Getting started

The latest version of the library is available at its `master` branch. All development, including new features and bug fixes, take place on the `master` branch using forking and pull requests as described in contribution guidelines.


### Installation

If you use `rebar3` you can include the library in your project with

```erlang
{elasticlog, ".*",
   {git, "https://github.com/fogfish/elasticlog", {branch, master}}
}
```

### Usage

The library requires Elastic search as a storage back-end. Use the docker container for development purpose.

```bash
docker run -it -p 9200:9200 fogfish/elasticsearch:6.2.3
```

Build library and run the development console

```bash
make && make run
```

Let's **create** a new index suitable for linked data and upload example [movie dataset](https://github.com/fogfish/datalog/blob/master/priv/imdb.nt).

```erlang
elasticlog:start().

%% 
%% establish connection with `imdb` index 
{ok, Sock} = esio:socket("http://localhost:9200/imdb").

%%
%% deploy a schema
elasticlog:schema(Sock, #{
   <<"schema:name">> => <<"xsd:string">>,
   <<"schema:born">> => <<"xsd:string">>,
   <<"schema:death">> => <<"xsd:string">>,
   <<"schema:title">> => <<"xsd:string">>,
   <<"schema:year">> => <<"xsd:integer">>,
   <<"schema:director">> => <<"xsd:anyURI">>,
   <<"schema:cast">> => <<"xsd:anyURI">>,
   <<"schema:sequel">> => <<"xsd:anyURI">>
}).

%%
%% open a stream to example dataset
Stream = semantic:nt(filename:join([code:priv_dir(datalog), "imdb.nt"])).

%%
%% upload dataset
%% Note: elasticlog group all facts about same subject into JSON document
%%       therefore preprocessing of N-triple stream is required.
%%       A raw stream of facts needs to be transformed into type-safe format
%%       and grouped together.
stream:foreach(
   fun(Fact)-> elasticlog:append(Sock, Fact) end,
   semantic:fold(
      stream:map(fun semantic:typed/1, Stream)
   )
).
```

Querying and joining semantical data requires translation of facts into n-ary relation. 
The library implements a σ function (`.stream`) that produces a stream of tuple from any 
bucket. It takes a name of bucket as first argument followed by predicate names.


**Basic queries**

```erlang
%%
%% define a query goal to match a person with `name` equal to `Ridley Scott`.
Q = "?- person(_, \"Ridley Scott\").
person(id, name) :- 
   .stream(\"imdb\", \"rdf:id\", \"schema:name\").".

%%
%% parse and compile a query into executable function
F = datalog:c(elasticlog, datalog:p(Q)).

%%
%% apply the function to dataset and materialize a stream of tuple, it returns
%% [
%%    [{iri,<<"http://example.org/person/137">>}, <<"Ridley Scott">>]
%% ]
stream:list(elasticlog:q(F, Sock)).
```

**Data patterns**

```erlang
%%
%% define a query to discover all movies produces in 1987
Q = "?- h(_, _). 
movie(id, title, year) :- 
   .stream(\"imdb\", \"rdf:id\", \"schema:title\", \"schema:year\").

h(id, title) :- 
   movie(id, title, 1987).".

%%
%% parse and compile a query into executable function
F = datalog:c(elasticlog, datalog:p(Q)).

%%
%% apply the function to dataset and materialize a stream of tuple, it returns
%% [
%%    [{iri,<<"http://example.org/movie/203">>}, <<"Lethal Weapon">>],
%%    [{iri,<<"http://example.org/movie/204">>}, <<"RoboCop">>],
%%    [{iri,<<"http://example.org/movie/202">>}, <<"Predator">>]
%% ]
stream:list(elasticlog:q(F, Sock)).
```

**Predicates**

```erlang
%%
%% define a query to discover all movies produces  before 1984
Q = "?- h(_, _). 
movie(title, year) :- 
   .stream(\"imdb\", \"schema:title\", \"schema:year\").

h(title, year) :- 
   movie(title, year), year < 1984.".

%%
%% parse and compile a query into executable function
F = datalog:c(elasticlog, datalog:p(Q)).

%%
%%
%% apply the function to dataset and materialize a stream of tuple, it returns
%% [
%%    [<<"First Blood">>, 1982],
%%    [<<"Mad Max">>, 1979],
%%    [<<"Mad Max 2">>, 1981],
%%    [<<"Alien">>, 1979]
%% ]
stream:list(elasticlog:q(F, Sock)).
```

**Join relations**

```erlang
%%
%% define a query to discover actors of all movies produced before 1984
Q = "?- h(_, _). 
movie(title, year) :- 
   .stream(\"imdb\", \"schema:title\", \"schema:year\", \"schema:cast\").

person(id, name) :- 
   .stream(\"imdb\", \"rdf:id\", \"schema:name\").

h(title, name) :- 
   movie(title, year, cast), 
   .flat(cast), 
   person(cast, name), 
   year < 1984.".

%%
%% parse and compile a query into executable function
F = datalog:c(elasticlog, datalog:p(Q)).

%%
%%
%% apply the function to dataset and materialize a stream of tuple, it returns
%% [
%%    [<<"First Blood">>,<<"Sylvester Stallone">>],
%%    [<<"First Blood">>,<<"Richard Crenna">>],
%%    [<<"First Blood">>,<<"Brian Dennehy">>],
%%    [<<"Mad Max">>,<<"Mel Gibson">>],
%%    [<<"Mad Max">>,<<"Steve Bisley">>],
%%    [<<"Mad Max">>,<<"Joanne Samuel">>],
%%    [<<"Mad Max 2">>,<<"Mel Gibson">>],
%%    [<<"Mad Max 2">>,<<"Michael Preston">>],
%%    [<<"Mad Max 2">>,<<"Bruce Spence">>],
%%    [<<"Alien">>,<<"Tom Skerritt">>],
%%    [<<"Alien">>,<<"Sigourney Weaver">>],
%%    [<<"Alien">>,<<"Veronica Cartwright">>]
%% ]
stream:list(elasticlog:q(F, Sock)).
```

```erlang
%%
%% define a query to discover all movies with Sylvester Stallone
Q = "?- h(_). 
movie(title, cast) :- 
   .stream(\"imdb\", \"schema:title\", \"schema:cast\").

person(id, name) :- 
   .stream(\"imdb\", \"rdf:id\", \"schema:name\").

h(title) :- 
   person(id, name), 
   movie(title, id), 
   name = \"Sylvester Stallone\".".

%%
%% parse and compile a query into executable function
F = datalog:c(elasticlog, datalog:p(Q)).

%%
%%
%% apply the function to dataset and materialize a stream of tuple, it returns
%% [
%%    [<<"First Blood">>],
%%    [<<"Rambo: First Blood Part II">>],
%%    [<<"Rambo III">>]
%% ]
stream:list(elasticlog:q(F, Sock)).
```

**Aggregations**

Elastic has an [aggregation framework](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html) to aggregated data based on a search query. Aggregations is an essential tool for analytical purposes. The library uses similar concept of n-ary relations, there is a σ function (`.select`) that takes bucket, its keys and aggregations directives and produces a stream of tuples where each element is aggregated value:

```
x(_) :-
   .select("bucket", "key", ...), %% defines a tuple's schema
   category()                     %% defines an aggregation function for first element
   count()                        %% defined an aggregation function for second element
   ...
   .
```

```erlang
%%
%% define a query to count release per decade
Q = "?- movie(_). 
movie(year) :- 
   .select(\"imdb\", \"schema:year\"), histogram(10).".

%%
%% parse and compile a query into executable function
F = datalog:c(elasticlog, datalog:p(Q)).

%%
%%
%% apply the function to dataset and materialize a stream of tuple, it returns
%% [
%%    [#{<<"count">> =>  2, <<"key">> => 1970.0}],
%%    [#{<<"count">> => 13, <<"key">> => 1980.0}],
%%    [#{<<"count">> =>  4, <<"key">> => 1990.0}],
%%    [#{<<"count">> =>  1, <<"key">> => 2.0e3}]]
%% ]
stream:list(elasticlog:q(F, Sock)).
```

```erlang
%%
%% define a query to count releases by 5 top directors
Q = "?- h(_, _). 
movie(id) :- 
   .select(\"imdb\", \"schema:director\"), category(5).

person(id, name) :- 
   .stream(\"imdb\", \"rdf:id\", \"schema:name\").

h(name, id) :-
   movie(id),
   person(id, name).".

%%
%% parse and compile a query into executable function
F = datalog:c(elasticlog, datalog:p(Q)).

%%
%%
%% apply the function to dataset and materialize a stream of tuple, it returns
%% [
%%    [<<"James Cameron">>, #{<<"count">> => 3, <<"key">> => <<"http://example.org/person/100">>}],
%%    [<<"Richard Donner">>,#{<<"count">> => 3, <<"key">> => <<"http://example.org/person/111">>}],
%%    [<<"George Miller">>, #{<<"count">> => 3, <<"key">> => <<"http://example.org/person/142">>}],
%%    [<<"John McTiernan">>,#{<<"count">> => 2, <<"key">> => <<"http://example.org/person/108">>}],
%%    [<<"Ted Kotcheff">>,  #{<<"count">> => 1, <<"key">> => <<"http://example.org/person/104">>}]
%% ]
stream:list(elasticlog:q(F, Sock)).
```

**Implicit**

The library support an implicit query constrains. It supports a use-case where confidential server-side needs to rewrite queries originated by public client (e.g. inject scopes or security constrains). The example below shows usage of implicit. The constrains `dc:publisher is imdb` is implicitly injected to each query supplied by the client.

```erlang
%%
%% define a query goal to match a person with `name` equal to `Ridley Scott`.
Q = "?- person(_, \"Ridley Scott\").
person(id, name) :- 
   .stream(\"imdb\", \"rdf:id\", \"schema:name\").".

%%
%% parse and compile a query into executable function
F = datalog:c(elasticlog, datalog:p(Q)).

%%
%% apply the function to dataset with implicit query.
stream:list(elasticlog:q(F, #{<<"dc:publisher">> => <<"imdb">>}, Sock)).
```


### More Information

* study datalog language and its [Erlang implementation](https://github.com/fogfish/datalog)


## How to Contribute

`elasticlog` is Apache 2.0 licensed and accepts contributions via GitHub pull requests.

### getting started

* Fork the repository on GitHub
* Read the README.md for build instructions
* Make pull request

### commit message

The commit message helps us to write a good release note, speed-up review process. The message should address two question what changed and why. The project follows the template defined by chapter [Contributing to a Project](http://git-scm.com/book/ch5-2.html) of Git book.

>
> Short (50 chars or less) summary of changes
>
> More detailed explanatory text, if necessary. Wrap it to about 72 characters or so. In some contexts, the first line is treated as the subject of an email and the rest of the text as the body. The blank line separating the summary from the body is critical (unless you omit the body entirely); tools like rebase can get confused if you run the two together.
> 
> Further paragraphs come after blank lines.
> 
> Bullet points are okay, too
> 
> Typically a hyphen or asterisk is used for the bullet, preceded by a single space, with blank lines in between, but conventions vary here
>

## Bugs

If you detect a bug, please bring it to our attention via GitHub issues. Please make your report detailed and accurate so that we can identify and replicate the issues you experience:
- specify the configuration of your environment, including which operating system you're using and the versions of your runtime environments
- attach logs, screen shots and/or exceptions if possible
- briefly summarize the steps you took to resolve or reproduce the problem


## License

Copyright 2016 Dmitry Kolesnikov

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.




