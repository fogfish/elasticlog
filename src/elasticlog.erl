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
%%   elastic search datalog
-module(elasticlog).

-export([start/0]).
-export([
   schema/1,
   schema/2,
   c/1,
   horn/2
]).

%%
%%
start() ->
   application:ensure_all_started(elasticlog).

%%
%% build schema for semantic data
-spec schema([semantic:iri()]) -> #{}.
-spec schema([semantic:iri()], [_]) -> #{}.

schema(Spec) ->
   schema(Spec, []).

schema(Spec, Opts) ->
   elasticlog_schema:new(Spec, Opts).


%%
%% compile textual query
c(Datalog)
 when is_map(Datalog) ->
   datalog:c(elasticlog_q, Datalog);
c(Datalog)
 when is_list(Datalog) ->
   c(datalog:p(Datalog)).

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
