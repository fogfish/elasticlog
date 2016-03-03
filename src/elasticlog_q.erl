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
sigma(Sock, #{'@' := P, '_' := [S] = Head} = Pattern) ->
   % binary relation: subject --[p]--> _
   case Pattern of
      #{S := Sx} when not is_list(Sx) ->
         statement(Head, stream_by_ps(Sock, P, Sx))
   end;

sigma(Sock, #{'@' := P, '_' := [S, O] = Head} = Pattern) ->
   % binary relation: subject --[p]--> object
   case Pattern of
      #{S := Sx, O := Ox} ->
         statement(Head, stream_by_pso(Sock, P, Sx, Ox));
      #{S := Sx} when not is_list(Sx) ->
         statement(Head, stream_by_ps(Sock, P, Sx));
      #{O := Ox} ->
         statement(Head, stream_by_po(Sock, P, Ox))
   end;

sigma(Sock, #{'@' := P, '_' := [S, O, C | _] = Head} = Pattern) ->
   % binary relation: subject --[p, c]--> object (augmented with credibility)
   case Pattern of
      #{S := Sx, O := Ox, C := Cx} ->
         statement(Head, filter(Cx, stream_by_pso(Sock, P, Sx, Ox)));
      #{S := Sx, C := Cx} when not is_list(Sx) ->
         statement(Head, filter(Cx, stream_by_ps(Sock, P, Sx)));
      #{O := Ox, C := Cx} ->
         statement(Head, filter(Cx, stream_by_po(Sock, P, Ox)));
      #{S := Sx, O := Ox} ->
         statement(Head, stream_by_pso(Sock, P, Sx, Ox));
      #{S := Sx} when not is_list(Sx) ->
         statement(Head, stream_by_ps(Sock, P, Sx));
      #{O := Ox} ->
         statement(Head, stream_by_po(Sock, P, Ox))
   end.

%%
%%
stream_by_ps(Sock, P, S) ->
   esio:match(Sock, urn(P), #{s => S}).

stream_by_po(Sock, P, O) ->
   esio:match(Sock, urn(P), #{o => O}).

stream_by_pso(Sock, P, S, O) ->
   esio:match(Sock, urn(P), #{s => S, o => O}).

%%
%%
statement([S, O], Stream) ->
   stream:map(
      fun(X) ->
         #{
            S => maps:get(<<"s">>, maps:get(<<"_source">>, X)),
            O => maps:get(<<"o">>, maps:get(<<"_source">>, X))
         }
      end,
      Stream
   );

statement([S], Stream) ->
   stream:map(
      fun(X) ->
         #{
            S => maps:get(<<"s">>, maps:get(<<"_source">>, X))
         }
      end,
      Stream
   );

statement([S, O, C], Stream) ->
   stream:map(
      fun(X) ->
         #{
            S => maps:get(<<"s">>, maps:get(<<"_source">>, X)),
            O => maps:get(<<"o">>, maps:get(<<"_source">>, X)),
            C => maps:get(<<"_score">>, X)
         }
      end,
      Stream
   );

statement([S, O, C, K], Stream) ->
   stream:map(
      fun(X) ->
         #{
            S => maps:get(<<"s">>, maps:get(<<"_source">>, X)),
            O => maps:get(<<"o">>, maps:get(<<"_source">>, X)),
            C => maps:get(<<"_score">>, X),
            K => maps:get(<<"_id">>, X)
         }
      end,
      Stream
   ).


%%
%%
filter(C, Stream) ->
   stream:takewhile(
      fun(X) -> 
         maps:get(<<"_score">>, X) >= C
      end,
      Stream
   ).

%%
%%
urn(Pred) ->
   uri:segments([scalar:s(Pred)], {urn, <<"es">>, <<>>}).
