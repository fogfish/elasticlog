# elasticlog

[Datalog](https://en.wikipedia.org/wiki/Datalog) query support for Elastic Search using the [parser](https://github.com/fogfish/datalog) implemented on Erlang.

[![Build Status](https://secure.travis-ci.org/fogfish/elasticlog.svg?branch=master)](http://travis-ci.org/fogfish/elasticlog)

## Inspiration 

Declarative logic programs is an alternative approach to extract knowledge facts in deductive databases. It is designed for applications that uses a large number of ground facts persisted in external storage. The library implements experimental support of datalog language to query knowledge facts stored in Elastic Search using [elasticnt](https://github.com/fogfish/elasticnt) library.

## Getting started

The latest version of the library is available at its master branch. All development, including new features and bug fixes, take place on the master branch using forking and pull requests as described in contribution guidelines.

To use and develop the library you need:
* Erlang/OTP 18.x or later
* rebar3
* Elastic Search
* elasticnt


### Installation

If you use `rebar` you can include the library in your project with
```
{elasticlog, ".*",
   {git, "https://github.com/fogfish/elasticlog", {branch, master}}
}
```


### Running

See [elasticnt](https://github.com/fogfish/elasticnt) for instructions to intake data into Elastic Search cluster.

Build library and run the development console
```
make
make run
```

Let's run a simple datalog program to deduct data from the cluster
```erlang
%% 
%% start elastic search drivers
esio:start().

%%
%% open cluster connection, define implicit index identity
{ok, Sock} = esio:socket("http://192.168.99.100:9200/nt").

%%
%% compile query, using datalog program
Dlog = elasticlog:c("nt(S, P, O) :- f(S, P, O), O = \"Example\".").

%%
%% evaluate program, using cluster connection
datalog:q(Dlog, Sock).


%%
%% compile query, using native syntax of data log program
Elog = datalog:q(
   #{o => <<"Example">>},
   elasticlog:horn([s,p,o], [
      #{'@' => p, '_' => [s,p,o]}
   ])
).

%%
%% evaluate program, using cluster connection
Elog(Sock).
``` 

### Supported datalog predicates

#### `f(...)` pattern match 

The predicate matches the knowledge statement and lift unbound variable. There are variants of predicate:
* `f(S, P, O)` matches the knowledge statement (subject, predicate object) 
* `f(S, P, O, C)` matches the knowledge statement and its credibility  
* `f(S, P, O, C, K)` matches the knowledge statement, its credibility and K-order value 


#### `geo(...)` geohash match  

The predicate matches the knowledge statement of geohash type (object is GeoHash) .
* `geo(S, P, O, R)` matches the knowledge statement at given location (`O`) within the radius `R`
* `geo(S, P, Lat, Lng, R)` matches the knowledge statement at given location (`Lat`, `Lng`) within the radius `R`


### More Information
* study datalog language and its [Erlang implementation](https://github.com/fogfish/datalog)
* check [tips and hints](doc/howto.md)


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




