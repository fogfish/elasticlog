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

typeof(?XSD_DATETIME)-> #{type => date, format => basic_date_time_no_millis};
typeof(?XSD_DATE)    -> #{type => date, format => basic_date_time_no_millis};
typeof(?XSD_TIME)    -> #{type => date, format => basic_date_time_no_millis};
typeof(?XSD_YEARMONTH) -> #{type => date, format => basic_date_time_no_millis};
typeof(?XSD_YEAR)    -> #{type => date, format => basic_date_time_no_millis};

typeof(?XSD_MONTHDAY)-> #{type => text};
typeof(?XSD_MONTH)   -> #{type => text};
typeof(?XSD_DAY)     -> #{type => text};

typeof(?GEORSS_POINT) -> #{type => geo_point};
typeof(?GEORSS_HASH)  -> #{type => geo_point};
typeof({iri, ?LANG, _}) -> #{type => text}.


%%
%%
predicate(Json) ->
   maps:from_list(lists:flatten([schema_to_rdf(X) || X <- maps:to_list(Json)])).

schema_to_rdf({_, Schema}) ->
   Properties  = lens:get(lens_properties(), Schema),
   [{P, isa(lens:get(lens:at(<<"type">>), Type))} || {P, Type} <- maps:to_list(Properties)].

lens_properties() ->
   lens:c(lens:at(<<"mappings">>, #{}), lens:at(<<"_doc">>, #{}), lens:at(<<"properties">>, #{})).

isa(<<"keyword">>) -> ?XSD_ANYURI;
isa(<<"text">>) -> ?XSD_STRING;
isa(<<"long">>) -> ?XSD_INTEGER;
isa(<<"double">>) -> ?XSD_DECIMAL;
isa(<<"boolean">>) -> ?XSD_BOOLEAN;
isa(<<"date">>) -> ?XSD_DATETIME;
isa(<<"geo_point">>) -> ?GEORSS_HASH;
isa(_) -> undefined.



