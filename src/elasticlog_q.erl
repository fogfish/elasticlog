%%
%%   Copyright 2016 Dmitry Kolesnikov, All Rights Reserved
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
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

sigma(Sock, #{'@' := f, '_' := [_, _, _, C] = Head} = Pattern) ->
   Query  = q_build(Pattern),
   Filter = datalog:takewhile(C, Pattern),
   Stream = Filter(
      fun(X) -> maps:get(<<"_score">>, X) end, 
      esio:stream(Sock, {urn, <<"es">>, <<>>}, Query)
   ),
   statement(Head, Stream);

sigma(Sock, #{'@' := f, '_' := [_, _, _, C, _] = Head} = Pattern) ->
   Query = q_build(Pattern),
   Filter = datalog:takewhile(C, Pattern),
   Stream = Filter(
      fun(X) -> maps:get(<<"_score">>, X) end, 
      esio:stream(Sock, {urn, <<"es">>, <<>>}, Query)
   ),
   statement(Head, Stream);

sigma(Sock, #{'@' := f, '_' := Head} = Pattern) ->
   Query  = q_build(Pattern),
   Stream = esio:stream(Sock, {urn, <<"es">>, <<>>}, Query),
   statement(Head, Stream);

sigma(Sock, #{'@' := geo, '_' := [S, P, O, R]} = Pattern) ->
   Query   = q_build(Pattern#{'_' => [S, P]}),
   Filter  = #{
      geohash_cell => #{
         geohash => #{geohash => value(O, Pattern)},
         precision => value(R, Pattern),
         neighbors => true
      }
   },
   Stream = esio:stream(Sock, {urn, <<"es">>, <<"geohash">>},
      #{'query' => #{filtered => Query#{filter => Filter}}}
   ),
   statement([S, P, O], Stream);

sigma(Sock, #{'@' := geo, '_' := [S, P, Lat, Lng, R]} = Pattern) ->
   Query   = q_build(Pattern#{'_' => [S, P]}),
   Filter  = #{
      geohash_cell => #{
         geohash => #{geohash => hash:geo(value(Lat, Pattern), value(Lng, Pattern))},
         precision => value(R, Pattern),
         neighbors => true
      }
   },
   Stream = esio:stream(Sock, {urn, <<"es">>, <<"geohash">>},
      #{'query' => #{filtered => Query#{filter => Filter}}}
   ),
   geo_statement([S, P, Lat, Lng], Stream).

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


%%
%% return predicate schema (variable binding to keys) 
schema([S, P]) ->
   [{S, s}, {P, p}];
schema([S, P, O]) ->
   [{S, s}, {P, p}, {O, typeof(O)}];
schema([S, P, O, _]) ->
   [{S, s}, {P, p}, {O, typeof(O)}];
schema([S, P, O, _, K]) ->
   [{S, s}, {P, p}, {O, typeof(O)}, {K, k}].

typeof(X) when is_integer(X) -> long;
typeof(X) when is_float(X) -> double;
typeof(<<"true">>) -> boolean;
typeof(<<"false">>) -> boolean;
typeof(X) when is_binary(X) -> string.


%%
%% translate document to atomic statement
statement([S, P, O], Stream) ->
   stream:map(
      fun(X) ->
         Type = maps:get(<<"_type">>, X),
         Json = maps:get(<<"_source">>, X),
         #{
            S => maps:get(<<"s">>, Json), 
            P => maps:get(<<"p">>, Json), 
            O => maps:get(Type, Json)
         }
      end,
      Stream
   );

statement([S, P, O, C], Stream) ->
   stream:map(
      fun(X) ->
         Type = maps:get(<<"_type">>, X),
         Json = maps:get(<<"_source">>, X),
         #{
            S => maps:get(<<"s">>, Json), 
            P => maps:get(<<"p">>, Json), 
            O => maps:get(Type, Json),
            C => maps:get(<<"_score">>, X)
         }
      end,
      Stream
   );

statement([S, P, O, C, K], Stream) ->
   stream:map(
      fun(X) ->
         Type = maps:get(<<"_type">>, X),
         Json = maps:get(<<"_source">>, X),
         #{
            S => maps:get(<<"s">>, Json), 
            P => maps:get(<<"p">>, Json), 
            O => maps:get(Type, Json),
            C => maps:get(<<"_score">>, X),
            K => maps:get(<<"k">>, Json)
         }
      end,
      Stream
   ).

%%
%%
geo_statement([S, P, Lat, Lng], Stream) ->
   stream:map(
      fun(X) ->
         Type = maps:get(<<"_type">>, X),
         Json = maps:get(<<"_source">>, X),
         {LatX, LngX} = hash:geo(maps:get(Type, Json)),
         #{
            S => maps:get(<<"s">>, Json), 
            P => maps:get(<<"p">>, Json), 
            Lat => LatX,
            Lng => LngX 
         }
      end,
      Stream
   ).

