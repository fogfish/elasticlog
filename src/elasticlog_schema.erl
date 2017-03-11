-module(elasticlog_schema).
-include_lib("semantic/include/semantic.hrl").

-export([new/2]).

%%
%% define new schema
%%    n - number of replica (default 1)
%%    q - number of shards (default 8)
%%    
new(Spec, Opts) ->
   case opts:val(type, false, Opts) of
      false ->
         #{
            settings => #{
               number_of_shards   => opts:val(q, 8, Opts), 
               number_of_replicas => opts:val(n, 1, Opts)
            },
            mappings => maps:from_list([{to_json(IRI), schema(IRI)} || IRI <- Spec])
         };

      true ->
         maps:from_list([{to_json(IRI), schema(IRI)} || IRI <- Spec])
   end.

schema(IRI) ->
   #rdf_seq{seq = Seq} = semantic:lookup(IRI),
   #{
      properties => maps:from_list([{to_json(Id), elasticlog_q5x:schema(P)} || #rdf_property{id = Id} = P <- Seq])
   }.


%%
%%
to_json({iri, Prefix, Suffix}) ->
   <<Prefix/binary, $:, Suffix/binary>>;
to_json(X) ->
   scalar:s(X).

