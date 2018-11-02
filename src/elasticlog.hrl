
%%
%% 
-record(elasticlog, {
   implicit = undefined :: #{},   %% implicit restrictions 
   sock = undefined :: pid()      %% elastic search socket
}).