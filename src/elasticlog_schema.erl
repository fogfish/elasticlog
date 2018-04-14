-module(elasticlog_schema).

-include_lib("semantic/include/semantic.hrl").

-export([
   new/2
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

schema(Schema) ->
   [
      {<<"s">>, #{type => keyword}}
     |[{key(semantic:compact(scalar:s(P))), typeof(semantic:compact(scalar:s(Type)))} || {P, Type} <- maps:to_list(Schema)]
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
typeof(?GEORSS_HASH)  -> #{type => geo_point}.


%%
-spec key( semantic:iri() ) -> binary().

key({iri, Prefix, Suffix}) ->
   <<Prefix/binary, $:, Suffix/binary>>.

