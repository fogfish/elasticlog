%% @doc
%%   encode / decode spock knowledge statement to json
-module(elasticlog_codec).

-compile({parse_transform, category}).
-include_lib("semantic/include/semantic.hrl").

-export([
   encode/2,
   decode/2
]).


%%
%%
-spec encode(semantic:type(), _) -> _.

encode(Type, List)
 when is_list(List) ->
   [encode(Type, X) || X <- List];

encode(?XSD_ANYURI, Iri) -> 
   encode_iri(Iri);

encode(?XSD_DATETIME, Val) -> 
   scalar:s(tempus:encode(Val));

encode(?XSD_DATE, Val) -> 
   scalar:s(tempus:encode(Val));

encode(?XSD_TIME, Val) -> 
   scalar:s(tempus:encode(Val));

encode(?XSD_YEARMONTH, Val) -> 
   scalar:s(tempus:encode(Val));

encode(?XSD_YEAR, Val) -> 
   scalar:s(tempus:encode(Val));

encode(_, Val) -> 
   Val.

%%
encode_iri({iri, Prefix, Suffix}) ->
   <<Prefix/binary, $:, Suffix/binary>>;
encode_iri({iri, Urn}) ->
   Urn.

%%
%%
-spec decode(semantic:type(), _) -> _.


decode(Type, List)
 when is_list(List) ->
   [decode(Type, X) || X <- List];

decode(?XSD_ANYURI, Iri) -> 
   decode_iri(Iri);

decode(?XSD_DATETIME, Val) -> 
   tempus:decode(Val);

decode(?XSD_DATE, Val) -> 
   tempus:decode(Val);

decode(?XSD_TIME, Val) -> 
   tempus:decode(Val);

decode(?XSD_YEARMONTH, Val) -> 
   tempus:decode(Val);

decode(?XSD_YEAR, Val) -> 
   tempus:decode(Val);

decode(_, Val) -> 
   Val.


%%
decode_iri(<<Iri/binary>>) ->
   semantic:compact(Iri);
decode_iri(Iri) ->
   semantic:absolute(Iri).





