%% @doc
%%   datalog sigma function supports knowledge statement only.
%%   
%%   @todo: 
%%     * use max score for filtering
%%     * match by _id (subject) is key/val operation it is more efficient then match 
-module(elasticlog_q).

-export([
   sigma/2
]).

%%
%% return sigma function
sigma(Pred, Pattern) ->
   fun(Heap) ->
      fun(Sock) ->
         sigma(Sock, Pred, datalog:bind(Heap, Pattern))
      end
   end. 

%%
%%        
sigma(Sock, Pred, #{'_' := Head} = Pattern) ->
   statement(Head, 
      esio:match(Sock, urn(Pred), 
         pattern(Head, Pattern)
      )
   ).

statement([Id, Val], Stream) ->
   stream:map(
      fun(X) ->
         #{
            Id  => maps:get(<<"_id">>, X),
            Val => maps:get(<<"o">>, maps:get(<<"_source">>, X))
         }
      end,
      Stream
   ).

%%
%%
urn(Pred) ->
   uri:segments([scalar:s(Pred)], {urn, <<"es">>, <<>>}).

%%
%%
pattern([S, O | _], Heap) ->
   keycopy(S, '_id', Heap,
      keycopy(O, 'o', Heap, #{})
   ).

keycopy(KeyA, KeyB, A, B) ->
   case maps:find(KeyA, A) of
      error ->
         B;
      {ok, Val} ->
         B#{KeyB => Val}
   end.




