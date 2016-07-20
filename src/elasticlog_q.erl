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

sigma(Sock, #{'@' := Fun, '_' := Head} = Pattern) ->
   statement(Fun, Head,
      filter(Pattern,
         stream(Sock, Pattern)
      )
   ).

%%
%% build data stream from pattern
stream(Sock, #{'@' := geo} = Pattern) ->
   Query = q_build(Pattern),
   % io:format("~s~n", [jsx:encode(Query)]),
   esio:stream(Sock, {urn, <<"es">>, <<"geohash">>}, Query);

stream(Sock, Pattern) ->
   Query = q_build(Pattern),
   % io:format("~s~n", [jsx:encode(Query)]),
   esio:stream(Sock, {urn, <<"es">>, <<>>}, Query).

%%
%% elastic search query builder
q_build(Pattern) ->
   {Match, Filter} = q_split(Pattern),
   q_build(q_bool(Pattern, Match), q_filter(Pattern, Filter), q_sort(Pattern, Filter)).

q_build(Matcher, undefined, undefined) ->
   Matcher;
q_build(Matcher, Filters, undefined) ->   
   #{'query' => #{filtered => Matcher#{filter => Filters}}};
q_build(Matcher, Filters, Sort) ->   
   #{'query' => #{filtered => Matcher#{filter => Filters}}, sort => Sort}.


%%
%% split query to pattern match and filters
q_split(#{'@' := Fun, '_' := Head} = Pattern) ->
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
      schema(Fun, Head)
   ).

%%
%% build boolean query
q_bool(#{'@' := Fun}, Pattern) ->
   #{'query' => 
      #{bool => 
         #{must => [#{match => #{q_typeof(Key, Fun) => Val}} || {Key, Val} <- Pattern]}
      }
   }.

%%
%% build filter restrictions
q_filter(#{'@' := geo} = Pattern, _) ->
   % @todo: re-write it using proper functional concept
   %        geo predicate might lift value to result
   try
      #{
         geohash_cell => #{
            geohash => #{geohash => q_filter_geo_hash(Pattern)},
            precision => q_filter_geo_precision(Pattern),
            neighbors => true
         }
      }
   catch _:_ ->
      undefined
   end;

q_filter(_, []) ->
   undefined;
q_filter(#{'@' := Fun}, Filters) ->
   #{'and' => [q_filter_value(q_typeof(Key, Fun), Val) || {Key, Val} <- Filters]}.

q_filter_value(Field, Guard) ->
   #{range => #{Field => maps:from_list([{q_term(Op), Val} || {Op, Val} <- Guard])}}.

q_filter_geo_precision(#{'_' := [_, _, X|_]} = Pattern) ->
   value(X, Pattern).

q_filter_geo_hash(#{'_' := [_, _, _, Hash]} = Pattern) ->
   value(Hash, Pattern);

q_filter_geo_hash(#{'_' := [_, _, _, Lat, Lng]} = Pattern) ->
   hash:geo(value(Lat, Pattern), value(Lng, Pattern)).

%%
%%
q_sort(#{'@' := geo} = Pattern, _) ->
   % @todo: re-write it using proper functional concept
   %        geo predicate might lift value to result
   try
      #{
         '_geo_distance' => #{
            geohash => q_filter_geo_hash(Pattern),
            order => asc,
            unit => m,
            distance_type => plane
         }
      }
   catch _:_ ->
      undefined
   end;

q_sort(_, _) ->
   undefined.



%%
%%
q_term('>')  -> gt;
q_term('>=') -> gte; 
q_term('<')  -> lt;
q_term('=<') -> lte.

%%
%%
q_typeof(s, _) -> s;
q_typeof(p, _) -> p;
q_typeof(k, _) -> k;
%% todo: alias
q_typeof(o, t) -> datetime;
q_typeof(o, l) -> string;
q_typeof(o, int) -> integer;
q_typeof(o, bool) -> boolean;
q_typeof(o, Type) -> Type.


%%
%%
filter(#{'@' := geo}, Stream) ->
   Stream;
filter(#{'_' := [_, _, _, C]} = Pattern, Stream) ->
   Filter = datalog:takewhile(C, Pattern),
   Filter(fun(X) -> maps:get(<<"_score">>, X) end, Stream);
filter(#{'_' := [_, _, _, C, _]} = Pattern, Stream) ->
   Filter = datalog:takewhile(C, Pattern),
   Filter(fun(X) -> maps:get(<<"_score">>, X) end, Stream);
filter(_, Stream) ->
   Stream.


%%
%%
value(X, Pattern)
 when is_atom(X) ->
   maps:get(X, Pattern);
value(X, _Pattern) ->
   X.

%%
%% return predicate schema (variable binding to keys) 
schema(geo, [S, P|_]) ->
   [{S, s}, {P, p}];
schema(_, [S, P]) ->
   [{S, s}, {P, p}];
schema(_, [S, P, O]) ->
   [{S, s}, {P, p}, {O, o}];
schema(_, [S, P, O, _]) ->
   [{S, s}, {P, p}, {O, o}];
schema(_, [S, P, O, _, K]) ->
   [{S, s}, {P, p}, {O, o}, {K, k}].

%%
%% translate document to atomic statement
statement(geo, [S, P, _, Lat, Lng], Stream) ->
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
   );

statement(_, [S, P, O], Stream) ->
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

statement(_, [S, P, O, C], Stream) ->
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

statement(_, [S, P, O, C, K], Stream) ->
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
