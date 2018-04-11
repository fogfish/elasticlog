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
   Json#{'xsd:anyURI' => encode_iri(Iri)};

encode_o(#{o := Val, type := ?XSD_STRING}, Json) -> 
   Json#{'xsd:string' => Val};

encode_o(#{o := Val, type := ?XSD_INTEGER}, Json) -> 
   Json#{'xsd:integer' => Val};

encode_o(#{o := Val, type := ?XSD_BYTE}, Json) -> 
   Json#{'xsd:byte' => Val};

encode_o(#{o := Val, type := ?XSD_BYTE}, Json) -> 
   Json#{'xsd:byte' => Val};

encode_o(#{o := Val, type := ?XSD_SHORT}, Json) -> 
   Json#{'xsd:short' => Val};

encode_o(#{o := Val, type := ?XSD_INT}, Json) -> 
   Json#{'xsd:int' => Val};

encode_o(#{o := Val, type := ?XSD_LONG}, Json) -> 
   Json#{'xsd:long' => Val};

encode_o(#{o := Val, type := ?XSD_DECIMAL}, Json) -> 
   Json#{'xsd:decimal' => Val};

encode_o(#{o := Val, type := ?XSD_FLOAT}, Json) -> 
   Json#{'xsd:float' => Val};

encode_o(#{o := Val, type := ?XSD_DOUBLE}, Json) -> 
   Json#{'xsd:double' => Val};

encode_o(#{o := Val, type := ?XSD_BOOLEAN}, Json) -> 
   Json#{'xsd:boolean' => Val};

encode_o(#{o := Val, type := ?XSD_DATETIME}, Json) -> 
   Json#{'xsd:dateTime' => scalar:s(tempus:encode(Val))};

encode_o(#{o := Val, type := ?XSD_DATE}, Json) -> 
   Json#{'xsd:date' => scalar:s(tempus:encode(Val))};

encode_o(#{o := Val, type := ?XSD_TIME}, Json) -> 
   Json#{'xsd:time' => scalar:s(tempus:encode(Val))};

encode_o(#{o := Val, type := ?XSD_YEARMONTH}, Json) -> 
   Json#{'xsd:gYearMonth' => scalar:s(tempus:encode(Val))};

encode_o(#{o := Val, type := ?XSD_YEAR}, Json) -> 
   Json#{'xsd:gYear' => scalar:s(tempus:encode(Val))};

encode_o(#{o := Val, type := ?XSD_MONTHDAY}, Json) -> 
   Json#{'xsd:gMonthDay' => Val};

encode_o(#{o := Val, type := ?XSD_MONTH}, Json) -> 
   Json#{'xsd:gMonth' => Val};

encode_o(#{o := Val, type := ?XSD_DAY}, Json) -> 
   Json#{'xsd:gDay' => Val};

encode_o(#{o := Val, type := ?GEORSS_POINT}, Json) -> 
   Json#{'georss:point' => Val};

encode_o(#{o := Val, type := ?GEORSS_HASH}, Json) -> 
   Json#{'georss:hash' => Val};

encode_o(#{o := Val, type := ?IRI_LANG(_)}, Json) ->
   %% Note: this version do not support a language extension
   %%       all language typed literals are converted to xsd:string
   Json#{'xsd:string' => Val}.


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
decode_o(#{<<"xsd:anyURI">> := Iri}, Spock) ->
   Spock#{o => decode_iri(Iri), type => ?XSD_ANYURI};

decode_o(#{<<"xsd:string">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_STRING};

decode_o(#{<<"xsd:integer">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_INTEGER};

decode_o(#{<<"xsd:byte">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_BYTE};

decode_o(#{<<"xsd:short">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_SHORT};

decode_o(#{<<"xsd:int">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_INT};

decode_o(#{<<"xsd:long">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_LONG};

decode_o(#{<<"xsd:decimal">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_DECIMAL};

decode_o(#{<<"xsd:float">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_FLOAT};

decode_o(#{<<"xsd:double">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_DOUBLE};

decode_o(#{<<"xsd:boolean">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_BOOLEAN};

decode_o(#{<<"xsd:dateTime">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_DATETIME};

decode_o(#{<<"xsd:date">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_DATE};

decode_o(#{<<"xsd:time">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_TIME};

decode_o(#{<<"xsd:gYearMonth">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_YEARMONTH};

decode_o(#{<<"xsd:gYear">> := Val}, Spock) ->
   Spock#{o => Val, type => ?XSD_YEAR};

decode_o(#{<<"xsd:gMonthDay">> := Val}, Spock) -> 
   Spock#{o => Val, type => ?XSD_MONTHDAY};

decode_o(#{<<"xsd:gMonth">> := Val}, Spock) -> 
   Spock#{o => Val, type => ?XSD_MONTH};

decode_o(#{<<"xsd:gDay">> := Val}, Spock) -> 
   Spock#{o => Val, type => ?XSD_DAY};

decode_o(#{<<"georss:point">> := Val}, Spock) ->
   Spock#{o => Val, type => ?GEORSS_POINT};

decode_o(#{<<"georss:hash">> := Val}, Spock) ->
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





