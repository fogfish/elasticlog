-module(elasticlog).

-export([
   c/1
]).

%%
%% compile query
c(Datalog) -> 
   datalog:c(elasticlog_q, datalog:p(Datalog)).

