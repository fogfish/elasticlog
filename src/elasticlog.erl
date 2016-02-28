-module(elasticlog).

-export([
   c/1,
   horn/2
]).

%%
%% compile textual query
c(Datalog) -> 
   datalog:c(elasticlog_q, datalog:p(Datalog)).


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
   datalog:horn(Head, [elasticlog_q:sigma(X) || X <- List]).
