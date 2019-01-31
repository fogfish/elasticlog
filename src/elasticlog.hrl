
%%
%% 
-record(elasticlog, {
   implicit    = undefined :: #{},     %% implicit restrictions
   equivalent  = undefined :: #{},     %% properties equivalent mapping
   sock        = undefined :: pid()    %% elastic search socket
}).