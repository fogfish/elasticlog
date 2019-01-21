-module(elasticlog_schema).

-include_lib("semantic/include/semantic.hrl").

-export([
   new/2,
   predicate/1
]).

%%
%% define new schema
%%    n - number of replica (default 1)
%%    q - number of shards (default 8)
%%    
new(Schema, Opts) ->
   #{
      settings => #{
         number_of_shards   => opts:val(q, 8, Opts), 
         number_of_replicas => opts:val(n, 1, Opts)
      },
      mappings => #{
         '_doc' => #{
            properties => maps:from_list(schema(Schema))
         }
      }
   }.

schema(Schema)
 when is_map(Schema) ->
   schema(maps:to_list(Schema));
schema(Schema)
 when is_list(Schema) ->
   [
      {<<"rdf:id">>, #{type => keyword}}
     |[{P, typeof(semantic:compact(Type))} || {P, Type} <- Schema]
   ].

%%
%%
-spec typeof( semantic:iri() ) -> atom().

typeof(?XSD_ANYURI) -> #{type => keyword};

typeof(?XSD_STRING) -> #{type => text};

typeof(?XSD_INTEGER) -> #{type => long};
typeof(?XSD_BYTE)    -> #{type => long};
typeof(?XSD_SHORT)   -> #{type => long};
typeof(?XSD_INT)     -> #{type => long};
typeof(?XSD_LONG)    -> #{type => long};

typeof(?XSD_DECIMAL) -> #{type => double};
typeof(?XSD_FLOAT)   -> #{type => double};
typeof(?XSD_DOUBLE)  -> #{type => double};

typeof(?XSD_BOOLEAN) -> #{type => boolean};

typeof(?XSD_DATETIME)-> #{type => date, format => <<"yyyy-MM-dd'T'HH:mm:ssZ||yyyyMMdd'T'HHmmssZ">>};
typeof(?XSD_DATE)    -> #{type => date, format => <<"yyyy-MM-dd||yyyyMMdd">>};
typeof(?XSD_TIME)    -> #{type => date, format => <<"HH:mm:ssZ||HHmmssZ">>};
typeof(?XSD_YEARMONTH) -> #{type => date, format => <<"yyyy-MM">>};
typeof(?XSD_YEAR)    -> #{type => date, format => <<"yyyy">>};

typeof(?XSD_MONTHDAY)-> #{type => date, format => <<"--MM-dd">>};
typeof(?XSD_MONTH)   -> #{type => date, format => <<"MM">>};
typeof(?XSD_DAY)     -> #{type => date, format => <<"dd">>};

typeof(?GEORSS_POINT) -> #{type => geo_point};
typeof(?GEORSS_HASH)  -> #{type => geo_point};
typeof(?GEORSS_JSON)  -> #{type => geo_shape};
typeof({iri, ?LANG, _}) -> #{type => text}.


%%
%%
predicate(Json) ->
   maps:from_list(lists:flatten([schema_to_rdf(X) || X <- maps:to_list(Json)])).

schema_to_rdf({_, Schema}) ->
   properties([], lens:get(lens_properties(), Schema)).

properties(Prefix, #{<<"properties">> := Properties}) ->
   [
      [{typecast:s(lists:join(<<".">>, Prefix)), undefined}]
   ,  [properties(Prefix ++ [P], Type) || {P, Type} <- maps:to_list(Properties)]
   ];
properties(Prefix, Type) ->
   [ {typecast:s(lists:join(<<".">>, Prefix)), isa(Type)} ];
properties(_, #{}) ->
   [].

lens_properties() ->
   lens:c(lens:at(<<"mappings">>, #{}), lens:at(<<"_doc">>, #{})).

isa(#{<<"type">> := <<"keyword">>}) -> ?XSD_ANYURI;
isa(#{<<"type">> := <<"text">>}) -> ?XSD_STRING;
isa(#{<<"type">> := <<"long">>}) -> ?XSD_INTEGER;
isa(#{<<"type">> := <<"double">>}) -> ?XSD_DECIMAL;
isa(#{<<"type">> := <<"boolean">>}) -> ?XSD_BOOLEAN;
isa(#{<<"type">> := <<"date">>, <<"format">> := <<"yyyy-MM-dd'T'HH:mm:ssZ||yyyyMMdd'T'HHmmssZ">>}) -> ?XSD_DATETIME;
isa(#{<<"type">> := <<"date">>, <<"format">> := <<"yyyy-MM-dd||yyyyMMdd">>}) -> ?XSD_DATE;
isa(#{<<"type">> := <<"date">>, <<"format">> := <<"HH:mm:ssZ||HHmmssZ">>}) -> ?XSD_TIME;
isa(#{<<"type">> := <<"date">>, <<"format">> := <<"yyyy-MM">>}) -> ?XSD_YEARMONTH;
isa(#{<<"type">> := <<"date">>, <<"format">> := <<"--MM-dd">>}) -> ?XSD_MONTHDAY;
isa(#{<<"type">> := <<"date">>, <<"format">> := <<"yyyy">>}) -> ?XSD_YEAR;
isa(#{<<"type">> := <<"date">>, <<"format">> := <<"MM">>}) -> ?XSD_MONTH;
isa(#{<<"type">> := <<"date">>, <<"format">> := <<"dd">>}) -> ?XSD_DAY;
isa(#{<<"type">> := <<"date">>}) -> ?XSD_DATETIME; % fallback to xsd_datetime if schema is defined outside of elasticlog 
isa(#{<<"type">> := <<"geo_shape">>}) -> ?GEORSS_JSON;
isa(#{<<"type">> := <<"geo_point">>}) -> ?GEORSS_POINT;
isa(_) -> undefined.



