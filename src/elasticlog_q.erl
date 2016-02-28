%% @doc
%%   datalog sigma function supports knowledge statement only.
-module(elasticlog_q).

-export([
   sigma/1
]).

%%
%% return sigma function
sigma(Pattern) ->
   fun(Heap) ->
      fun(Sock) ->
         sigma(Sock, datalog:bind(Heap, Pattern))
      end
   end. 

%%
%%        
sigma(Sock, #{'@' := Pred, '_' := [Key, Val] = Head} = Pattern) ->
   % binary relation: subject --[p]--> object
   case Pattern of
      #{Key := K, Val := V} ->
         statement(Head, stream_by_kv(Sock, Pred, K, V));
      #{Key := K} when not is_list(K) ->
         statement(Head, stream_by_k(Sock, Pred, K));
      #{Val := V} ->
         statement(Head, stream_by_v(Sock, Pred, V))
   end;

sigma(Sock, #{'@' := Pred, '_' := [Key, Val, Crd] = Head} = Pattern) ->
   % binary relation: subject --[p, c]--> object (augmented with credibility)
   case Pattern of
      #{Key := K, Val := V, Crd := C} ->
         statement(Head, filter(C, stream_by_kv(Sock, Pred, K, V)));
      #{Key := K, Crd := C} when not is_list(K) ->
         statement(Head, filter(C, stream_by_k(Sock, Pred, K)));
      #{Val := V, Crd := C} ->
         statement(Head, filter(C, stream_by_v(Sock, Pred, V)));
      #{Key := K, Val := V} ->
         statement(Head, stream_by_kv(Sock, Pred, K, V));
      #{Key := K} when not is_list(K) ->
         statement(Head, stream_by_k(Sock, Pred, K));
      #{Val := V} ->
         statement(Head, stream_by_v(Sock, Pred, V))
   end.

%%
%%
stream_by_k(Sock, Pred, Key) ->
   esio:match(Sock, urn(Pred), #{'_id' => Key}).

stream_by_v(Sock, Pred, Val) ->
   esio:match(Sock, urn(Pred), #{'o' => Val}).

stream_by_kv(Sock, Pred, Key, Val) ->
   esio:match(Sock, urn(Pred), #{'_id' => Key, o => Val}).

%%
%%
statement([Key, Val | _], Stream) ->
   stream:map(
      fun(X) ->
         #{
            Key => maps:get(<<"_id">>, X),
            Val => maps:get(<<"o">>, maps:get(<<"_source">>, X))
         }
      end,
      Stream
   ).

%%
%%
filter(Crd, Stream) ->
   stream:takewhile(
      fun(X) -> 
         maps:get(<<"_score">>, X) >= Crd 
      end,
      Stream
   ).

%%
%%
urn(Pred) ->
   uri:segments([scalar:s(Pred)], {urn, <<"es">>, <<>>}).
