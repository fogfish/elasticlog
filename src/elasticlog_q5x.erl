%%
%% @doc
%%   Elastic Search 5.x query builder
-module(elasticlog_q5x).

-include_lib("semantic/include/semantic.hrl").
-compile({parse_transform, category}).

-export([build/2]).

build(#rdf_seq{seq = Seq}, #{'_' := Head} = Pattern) ->
   Spec    = lists:zip(Seq, Head),
   Filters = q_filters( filters(Spec, Pattern) ),
   Matches = q_matches( matches(Spec, Pattern) ),
   #{'query' => #{bool => #{must => Matches, filter => Filters}}}.

%%
%%
filters([Head | Tail], Pattern) ->
   case filter(Head, Pattern) of
      undefined ->
         filters(Tail, Pattern);
      Value ->
         [Value | filters(Tail, Pattern)]
   end;
filters([], _) ->
   [].

filter({#rdf_property{} = Spec, Heap}, Pattern)
 when is_atom(Heap) ->
   %% heap value is not defined, this is reference to heap
   case Pattern of
      %% the value is filter constrain (range)
      #{Heap := Value} when is_list(Value) -> 
         {range, Spec, Value};

      %% pattern match
      #{Heap := Value} ->
         {terms, Spec, [Value]};

      %% unbound variable
      _ ->
         undefined
   end;

filter({#rdf_property{} = Spec, Value}, _) ->
   {terms, Spec, [Value]}.

%%
%%
q_filters(Spec) ->
   lists:map(fun q_filter/1, Spec).

q_filter({range, #rdf_property{id = IRI}, Value}) ->
   #{range => #{to_json(IRI) => maps:from_list([{q_guard(Guard), X} || {Guard, X} <- Value])}};

q_filter({terms, #rdf_property{id = IRI}, Value}) ->
   #{terms => #{to_json(IRI) => Value}}.


%%
%%
matches([Head | Tail], Pattern) ->
   case match(Head, Pattern) of
      undefined ->
         matches(Tail, Pattern);
      Value ->
         [Value | matches(Tail, Pattern)]
   end;
matches([], _) ->
   [].

match({#rdf_property{datatype = {iri, ?LANG, _}} = Spec, Heap}, Pattern)
 when is_atom(Heap) ->
   %% heap value is not defined, this is reference to heap
   case Pattern of
      %% pattern match
      #{Heap := Value} when not is_list(Value) ->
         {terms, Spec, [Value]};

      %% unbound variable
      _ ->
         undefined
   end;

match({#rdf_property{datatype = {iri, ?LANG, _}} = Spec, Value}, _) ->
   {terms, Spec, [Value]};

match(_, _) ->
   undefined.


%%
%%
q_matches(Spec) ->
   lists:map(fun q_match/1, Spec).

q_match({terms, #rdf_property{id = IRI}, [Value]}) ->
   #{match => #{to_json(IRI) => Value}}.


%%
%%
q_guard('>')  -> gt;
q_guard('>=') -> gte; 
q_guard('<')  -> lt;
q_guard('=<') -> lte.

%%
%%
to_json({iri, Prefix, Suffix}) ->
   <<Prefix/binary, $:, Suffix/binary>>.   


