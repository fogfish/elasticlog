-module(elasticlog_schema).

-include_lib("semantic/include/semantic.hrl").

-export([
   new/1
]).

%%
%% define new schema
%%    n - number of replica (default 1)
%%    q - number of shards (default 8)
%%    
new(Opts) ->
   #{
      settings => #{
         number_of_shards   => opts:val(q, 8, Opts), 
         number_of_replicas => opts:val(n, 1, Opts)
      },
      mappings => #{
         '_default_' => #{
            properties => maps:from_list(schema())
         }
      }
   }.

schema() ->
   [
      {<<"s">>, #{type => keyword}}
     ,{<<"p">>, #{type => keyword}}
     ,{<<"c">>, #{type => double}}
     ,{<<"k">>, #{type => text}}
     |[{elasticlog_property:encode(X), typeof(X)} || X <- datatypes()]
   ].

datatypes() ->
   [
      ?XSD_ANYURI,
      ?XSD_STRING,
      ?XSD_INTEGER,
      ?XSD_BYTE,
      ?XSD_SHORT,
      ?XSD_INT,
      ?XSD_LONG,
      ?XSD_DECIMAL,
      ?XSD_FLOAT,
      ?XSD_DOUBLE,
      ?XSD_BOOLEAN,
      ?XSD_DATETIME,
      ?XSD_DATE,
      ?XSD_TIME,
      ?XSD_YEARMONTH,
      ?XSD_YEAR,
      % ?XSD_MONTHDAY,
      % ?XSD_MONTH,
      % ?XSD_DAY,
      ?GEORSS_POINT,
      ?GEORSS_HASH   
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

% typeof(?XSD_MONTHDAY)-> <<"xsd_gmonthday">>;
% typeof(?XSD_MONTH)   -> <<"xsd_gmonth">>;
% typeof(?XSD_DAY)     -> <<"xsd_gday">>;

typeof(?GEORSS_POINT) -> #{type => geo_point};
typeof(?GEORSS_HASH)  -> #{type => geo_point}.

