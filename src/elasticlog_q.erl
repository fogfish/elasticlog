%% @doc
%%   datalog sigma function supports knowledge statement only.
-module(elasticlog_q).

-export([
   sigma/1
]).

%%
%% build sigma function
sigma(Pattern) ->
   fun(Heap) ->
      fun(Sock) ->
         sigma(Sock, datalog:bind(Heap, Pattern))
      end
   end. 

sigma(Sock, #{'@' := p, '_' := [_, _, _, C] = Head} = Pattern) ->
   Query  = q_build(Pattern),
   Filter = datalog:takewhile(C, Pattern),
   Stream = Filter(
      fun(X) -> maps:get(<<"_score">>, X) end, 
      esio:stream(Sock, {urn, <<"es">>, <<>>}, Query)
   ),
   statement(Head, Stream);

sigma(Sock, #{'@' := p, '_' := [_, _, _, C, _] = Head} = Pattern) ->
   Query = q_build(Pattern),
   Filter = datalog:takewhile(C, Pattern),
   Stream = Filter(
      fun(X) -> maps:get(<<"_score">>, X) end, 
      esio:stream(Sock, {urn, <<"es">>, <<>>}, Query)
   ),
   statement(Head, Stream);

sigma(Sock, #{'@' := p, '_' := Head} = Pattern) ->
   Query  = q_build(Pattern),
   Stream = esio:stream(Sock, {urn, <<"es">>, <<>>}, Query),
   statement(Head, Stream);

sigma(Sock, #{'@' := geo, '_' := [S, P, O, R] = Head} = Pattern) ->
   Query   = q_build(Pattern#{'_' => [S, P]}),
   Filter  = #{
      geohash_cell => #{
         o => #{geohash => value(O, Pattern)},
         precision => value(R, Pattern),
         neighbors => true
      }
   },
   Stream = esio:stream(Sock, {urn, <<"es">>, <<"geohash">>},
      #{'query' => #{filtered => Query#{filter => Filter}}}
   ),
   statement([S, P, O], Stream).

value(X, Pattern)
 when is_atom(X) ->
   maps:get(X, Pattern);
value(X, _Pattern) ->
   X.

%%
%% elastic search query builder
q_build(Pattern) ->
   case q_split(Pattern) of
      {Pat, []} ->
         q_bool(Pat);
      {Pat, Filter} ->
         Query = q_bool(Pat),
         #{'query' => #{filtered => Query#{filter => q_filter(Filter)}}}
   end.

%%
%% split query to pattern match and filters
q_split(#{'_' := Head} = Pattern) ->
   lists:foldl(
      fun
      ({Pkey, Jkey}, {P, F} = Acc) when is_atom(Pkey) ->
         %% predicate key is variable
         case Pattern of
            %% filter / constrain
            #{Pkey := Val} when is_list(Val) ->
               {P, [{Jkey, Val}|F]};

            %% pattern match
            #{Pkey := Val} ->
               {[{Jkey, Val}|P], F};
            
            %% unbound variable
            _ ->
               Acc
         end;
      ({Pkey, Jkey}, {P, F}) ->
         %% predicate variable is in-line value
         {[{Jkey, Pkey}|P], F}
      end,
      {[], []},
      schema(Head)
   ).


%%
%% build boolean query
q_bool(Pattern) ->
   #{'query' => 
      #{bool => 
         #{must => [#{match => #{Key => Val}} || {Key, Val} <- Pattern]}
      }
   }.

%%
%%
q_filter(Filter) ->
   #{'and' => [q_filter(Key, Val) || {Key, Val} <- Filter]}.

q_filter(Field, Guard) ->
   #{range => #{Field => maps:from_list([{q_term(Op), Val} || {Op, Val} <- Guard])}}.


q_term('>')  -> gt;
q_term('>=') -> gte; 
q_term('<')  -> lt;
q_term('=<') -> lte.





% sigma(Sock, #{'@' := geo, '_' := [S, P, O, Lat, Lng, R]} = Pattern) ->
%    Query = #{
%   'query' => #{
%       filtered => #{
%       'query' => #{
%         bool => #{
%           must => #{
%             match => #{
%               p => <<"urn:georss:point">>
%             }
%           }
%         }
%       },
%       filter => #{
%         geohash_cell => #{
%           o => #{
%             geohash => <<"ud9y21j3u76e">>
%           },
%           precision => <<"10km">>,
%           neighbors => true
%         }
%       }
%     }
%   }
% },
%    statement([S, P, O], esio:stream(Sock, {urn, <<"es">>, <<"geohash">>}, Query)).


%%
%% return predicate schema (variable binding to keys) 
schema([S, P]) ->
   [{S, s}, {P, p}];
schema([S, P, O]) ->
   [{S, s}, {P, p}, {O, o}];
schema([S, P, O, _]) ->
   [{S, s}, {P, p}, {O, o}];
schema([S, P, O, _, K]) ->
   [{S, s}, {P, p}, {O, o}, {K, k}].


%%
%% translate document to atomic statement
statement([S, P, O], Stream) ->
   stream:map(
      fun(X) ->
         Json = maps:get(<<"_source">>, X),
         #{
            S => maps:get(<<"s">>, Json), 
            P => maps:get(<<"p">>, Json), 
            O => maps:get(<<"o">>, Json)
         }
      end,
      Stream
   );

statement([S, P, O, C], Stream) ->
   stream:map(
      fun(X) ->
         Json = maps:get(<<"_source">>, X),
         #{
            S => maps:get(<<"s">>, Json), 
            P => maps:get(<<"p">>, Json), 
            O => maps:get(<<"o">>, Json),
            C => maps:get(<<"_score">>, X)
         }
      end,
      Stream
   );

statement([S, P, O, C, K], Stream) ->
   stream:map(
      fun(X) ->
         Json = maps:get(<<"_source">>, X),
         #{
            S => maps:get(<<"s">>, Json), 
            P => maps:get(<<"p">>, Json), 
            O => maps:get(<<"o">>, Json),
            C => maps:get(<<"_score">>, X),
            K => maps:get(<<"k">>, Json)
         }
      end,
      Stream
   ).







% %%
% %% keep temporary for credibility filtering
% sigma(Sock, #{'@' := P, '_' := [S] = Head} = Pattern) ->
%    % binary relation: subject --[p]--> _
%    case Pattern of
%       #{S := Sx} when not is_list(Sx) ->
%          statement(Head, stream_by_ps(Sock, P, Sx))
%    end;

% sigma(Sock, #{'@' := P, '_' := [S, O] = Head} = Pattern) ->
%    % binary relation: subject --[p]--> object
%    case Pattern of
%       #{S := Sx, O := Ox} ->
%          statement(Head, stream_by_pso(Sock, P, Sx, Ox));
%       #{S := Sx} when not is_list(Sx) ->
%          statement(Head, stream_by_ps(Sock, P, Sx));
%       #{O := Ox} ->
%          statement(Head, stream_by_po(Sock, P, Ox))
%    end;

% sigma(Sock, #{'@' := P, '_' := [S, O, C | _] = Head} = Pattern) ->
%    % binary relation: subject --[p, c]--> object (augmented with credibility)
%    case Pattern of
%       #{S := Sx, O := Ox, C := Cx} ->
%          statement(Head, filter(Cx, stream_by_pso(Sock, P, Sx, Ox)));
%       #{S := Sx, C := Cx} when not is_list(Sx) ->
%          statement(Head, filter(Cx, stream_by_ps(Sock, P, Sx)));
%       #{O := Ox, C := Cx} ->
%          statement(Head, filter(Cx, stream_by_po(Sock, P, Ox)));
%       #{S := Sx, O := Ox} ->
%          statement(Head, stream_by_pso(Sock, P, Sx, Ox));
%       #{S := Sx} when not is_list(Sx) ->
%          statement(Head, stream_by_ps(Sock, P, Sx));
%       #{O := Ox} ->
%          statement(Head, stream_by_po(Sock, P, Ox))
%    end.





% %%
% %%
% urn(Pred) ->
%    uri:segments([scalar:s(Pred)], {urn, <<"es">>, <<>>}).
