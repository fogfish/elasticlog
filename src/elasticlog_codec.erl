%% @doc
%%   encode / decode spock knowledge statement to json
-module(elasticlog_codec).

-compile({parse_transform, category}).
-include_lib("semantic/include/semantic.hrl").

-export([
   encode/1,
   decode/1
]).


%%
%%
-spec encode( semantic:spock() ) -> map().

encode(Spock) ->
   [identity ||
      cats:unit(#{}),
      encode_s(Spock, _),
      encode_p(Spock, _),
      encode_o(Spock, _),
      encode_c(Spock, _),
      encode_k(Spock, _)
   ].

%%
encode_s(#{s := Iri}, Json) -> 
   Json#{s => encode_iri(Iri)}.

%%
encode_p(#{p := Iri}, Json) -> 
   Json#{p => encode_iri(Iri)}.

%%
encode_o(#{o := Iri, type := ?XSD_ANYURI}, Json) -> 
   Json#{xsd_anyuri => encode_iri(Iri)};

encode_o(#{o := Val, type := ?XSD_STRING}, Json) -> 
   Json#{xsd_string => Val};

encode_o(#{o := Val, type := ?XSD_INTEGER}, Json) -> 
   Json#{xsd_integer => Val};

encode_o(#{o := Val, type := ?XSD_BYTE}, Json) -> 
   Json#{xsd_byte => Val};

encode_o(#{o := Val, type := ?XSD_BYTE}, Json) -> 
   Json#{xsd_byte => Val};

encode_o(#{o := Val, type := ?XSD_SHORT}, Json) -> 
   Json#{xsd_short => Val};

encode_o(#{o := Val, type := ?XSD_INT}, Json) -> 
   Json#{xsd_int => Val};

encode_o(#{o := Val, type := ?XSD_LONG}, Json) -> 
   Json#{xsd_long => Val};

encode_o(#{o := Val, type := ?XSD_DECIMAL}, Json) -> 
   Json#{xsd_decimal => Val};

encode_o(#{o := Val, type := ?XSD_FLOAT}, Json) -> 
   Json#{xsd_float => Val};

encode_o(#{o := Val, type := ?XSD_DOUBLE}, Json) -> 
   Json#{xsd_double => Val};

encode_o(#{o := Val, type := ?XSD_BOOLEAN}, Json) -> 
   Json#{xsd_boolean => Val};

encode_o(#{o := Val, type := ?XSD_DATETIME}, Json) -> 
   Json#{xsd_datetime => scalar:s(tempus:encode(Val))};

encode_o(#{o := Val, type := ?XSD_DATE}, Json) -> 
   Json#{xsd_date => scalar:s(tempus:encode(Val))};

encode_o(#{o := Val, type := ?XSD_TIME}, Json) -> 
   Json#{xsd_time => scalar:s(tempus:encode(Val))};

encode_o(#{o := Val, type := ?XSD_YEARMONTH}, Json) -> 
   Json#{xsd_yearmonth => scalar:s(tempus:encode(Val))};

encode_o(#{o := Val, type := ?XSD_YEAR}, Json) -> 
   Json#{xsd_year => scalar:s(tempus:encode(Val))};

encode_o(#{o := Val, type := ?GEORSS_POINT}, Json) -> 
   Json#{georss_point => Val};

encode_o(#{o := Val, type := ?GEORSS_HASH}, Json) -> 
   Json#{georss_hash => Val}.

%%
encode_c(#{c := Val}, Json) -> 
   Json#{c => Val}.

%%
encode_k(#{k := Val}, Json) -> 
   Json#{k => Val}.

%%
encode_iri({iri, Prefix, Suffix}) ->
   <<"_:", Prefix/binary, $:, Suffix/binary>>;
encode_iri({iri, Urn}) ->
   Urn.

%%
%%
-spec decode( semantic:spock() ) -> map().

decode(Json) ->
   [identity ||
      cats:unit(#{}),
      decode_s(Json, _),
      decode_p(Json, _),
      decode_o(Json, _),
      decode_c(Json, _),
      decode_k(Json, _)
   ].

%%
decode_s(#{<<"s">> := Iri}, Spock) -> 
   Spock#{s => decode_iri(Iri)}.

%%
decode_p(#{<<"p">> := Iri}, Spock) -> 
   Spock#{p => decode_iri(Iri)}.

%%
decode_o(#{<<"xsd_anyuri">> := Iri}, Spock) ->
   Spock#{o => decode_iri(Iri), type => ?XSD_ANYURI};

decode_o(#{<<"xsd_string">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_STRING};

decode_o(#{<<"xsd_integer">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_INTEGER};

decode_o(#{<<"xsd_byte">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_BYTE};

decode_o(#{<<"xsd_short">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_SHORT};

decode_o(#{<<"xsd_int">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_INT};

decode_o(#{<<"xsd_long">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_LONG};

decode_o(#{<<"xsd_decimal">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_DECIMAL};

decode_o(#{<<"xsd_float">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_FLOAT};

decode_o(#{<<"xsd_double">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_DOUBLE};

decode_o(#{<<"xsd_boolean">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_BOOLEAN};

decode_o(#{<<"xsd_datetime">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_DATETIME};

decode_o(#{<<"xsd_date">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_DATE};

decode_o(#{<<"xsd_time">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_TIME};

decode_o(#{<<"xsd_gyearmonth">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_YEARMONTH};

decode_o(#{<<"xsd_gyear">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_YEAR};

decode_o(#{<<"georss_point">> := Val}, Spock) ->
   Spock#{o => Val, type => ?GEORSS_POINT};

decode_o(#{<<"georss_hash">> := Val}, Spock) ->
   Spock#{o => Val, type => ?GEORSS_HASH}.

%%
decode_c(#{<<"c">> := Val}, Spock) -> 
   Spock#{c => Val}.

%%
decode_k(#{<<"k">> := Val}, Spock) -> 
   Spock#{k => Val}.



%%
decode_iri(<<"_:", Iri/binary>>) ->
   semantic:compact(Iri);
decode_iri(Iri) ->
   semantic:absolute(Iri).





