%%
%% @doc
%%   Elastic Search 5.x query builder
-module(elasticlog_q5x).

-include_lib("semantic/include/semantic.hrl").
-compile({parse_transform, category}).

-export([
   schema/1,
   build/2
]).

%%
%%
schema(#rdf_property{datatype = ?XSD_ANYURI}) ->
   #{type => keyword};
schema(#rdf_property{datatype = ?XSD_STRING}) ->
   #{type => keyword};
schema(#rdf_property{datatype = ?RDF_LANG_STRING}) ->
   #{type => string};
schema(#rdf_property{datatype = ?XSD_INTEGER}) ->
   #{type => long};
schema(#rdf_property{datatype = ?XSD_DECIMAL}) ->
   #{type => double};
schema(#rdf_property{datatype = ?XSD_BOOLEAN}) ->
   #{type => boolean};
schema(#rdf_property{datatype = ?XSD_DATETIME}) ->
   #{type => date, format => strict_date_optional_time};
schema(#rdf_property{datatype = ?GEORSS_HASH}) ->
   #{type => geo_point};
schema(#rdf_property{}) ->
   #{type => keyword}.


%%
%% build elastic search query from datalog predicate
build(#rdf_seq{seq = Seq}, #{'_' := Head} = Pattern) ->
   Spec    = lists:zip(Seq, Head),
   Filters = [$.|| as_filters(Spec), filters(_, Pattern), q_filters(_)],
   Matches = [$.|| as_matches(Spec), matches(_, Pattern), q_matches(_)],
   #{'query' => #{bool => #{must => Matches, filter => Filters}}}.

%%
%%
as_filters(List) ->
   lists:filter(
      fun({#rdf_property{datatype = {iri, Type, Schema}}, _}) ->
         not (Type =:= ?LANG orelse Schema =:= ?LANG)
      end,
      List
   ).

as_matches(List) ->
   lists:filter(
      fun({#rdf_property{datatype = {iri, Type, Schema}}, _}) ->
         (Type =:= ?LANG orelse Schema =:= ?LANG)
      end,
      List
   ).
 

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

filter({_, '_'}, _) ->
   %% blank variable
   undefined;
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

q_filter({range, #rdf_property{id = IRI, datatype = ?GEORSS_HASH}, [GeoHash, Radius]}) ->
   #{geo_distance => #{distance => Radius, to_json(IRI) => GeoHash}};

% [geo_distance_range] queries are no longer supported for geo_point field types
% q_filter({range, #rdf_property{id = IRI, datatype = ?GEORSS_HASH}, [GeoHash, Inner, Radius]}) ->
%    #{geo_distance_range => #{from => Inner, to => Radius, to_json(IRI) => GeoHash}};

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

%% @todo: rdf:langString
match({_, '_'}, _) ->
   undefined;
match({#rdf_property{} = Spec, Heap}, Pattern)
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

match({#rdf_property{} = Spec, Value}, _) ->
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


