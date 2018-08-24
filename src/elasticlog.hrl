
%%
%% 
-record(elasticlog, {
   env  = undefined :: #{},   %% global environment
   sock = undefined :: pid()  %% elastic search socket
}).