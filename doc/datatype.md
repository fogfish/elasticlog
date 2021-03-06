# Data Types

Knowledge statements contain scalar objects -- literals. Literals are either language-tagged string `rdf:langString` or _type-safe_ values containing a reference to data-type (e.g. `xsd:string`). This section defines rules how semantic data types are serialized to elastic search.

## Primitive data types


### URI

Lang | Data type
---  | ---
Erlang | `-type {iri, uri()} \| {iri, prefix(), suffix()}.`
Semantic | `?XSD_ANYURI`
Elastic | `keyword`
Datalog | `xsd:anyURI(...)`
RDF | xmlns:xsd="http://www.w3.org/2001/XMLSchema#anyURI"



### binary

The binary data-type represents character strings in knowledge statements. The language strings are annotated with corresponding tag.

Lang | Data type
---  | ---
Erlang | `-type binary().`
Semantic | `?XSD_STRING`
Elastic | `text`
Datalog | `xsd:string(...)`
RDF | xmlns:xsd="http://www.w3.org/2001/XMLSchema#string"


### integer

The library derives an arbitrary-precision Integers from Erlang (only limited by available memory on the machine).

Lang | Data type
---  | ---
Erlang | `-type integer().`
Semantic | `?XSD_INTEGER`
|| `?XSD_LONG`
|| `?XSD_INT`
|| `?XSD_SHORT`
|| `?XSD_BYTE`
Elastic | `long`
Datalog | `xsd:integer(...)`
|| `xsd:long(...)`
|| `xsd:int(...)`
|| `xsd:short(...)`
|| `xsd:byte(...)`
RDF | xmlns:xsd="http://www.w3.org/2001/XMLSchema#integer"
|| xmlns:xsd="http://www.w3.org/2001/XMLSchema#long"
|| xmlns:xsd="http://www.w3.org/2001/XMLSchema#int"
|| xmlns:xsd="http://www.w3.org/2001/XMLSchema#short"
|| xmlns:xsd="http://www.w3.org/2001/XMLSchema#byte"


### float

The value is the IEEE 754 double-precision 64-bit floating point type.

Lang | Data type
---  | ---
Erlang | `-type float().`
Semantic | `?XSD_DECIMAL`
|| `?XSD_FLOAT`
|| `?XSD_DOUBLE`
Elastic | `double`
Datalog | `xsd:decimal(...)`
|| `xsd:float(...)`
|| `xsd:double(...)`
RDF | xmlns:xsd="http://www.w3.org/2001/XMLSchema#decimal"
|| xmlns:xsd="http://www.w3.org/2001/XMLSchema#float"
|| xmlns:xsd="http://www.w3.org/2001/XMLSchema#double"


### boolean

The value is either true or false, representing a logic values

Lang | Data type
---  | ---
Erlang | `-type true \| false.`
Semantic | `?XSD_BOOLEAN`
Elastic | `boolean`
Datalog | `xsd:boolean(...)`
RDF | xmlns:xsd="http://www.w3.org/2001/XMLSchema#boolean"


### datetime

The date-time value is closely related to calendar dates and times described by ISO 8601, covering AD and BC eras. Erlang native date format is triple of integers giving micro-seconds precision (see tempus interface). The external representation is ISO 8601. 

Lang | Data type
---  | ---
Erlang | `-type {integer(), integer(), integer()}.`
Semantic | `?XSD_DATETIME`
|| `?XSD_DATE`
|| `?XSD_TIME`
|| `?XSD_YEARMONTH`
|| `?XSD_YEAR`
Elastic | `date`
Datalog | `xsd:datetime(...)`
|| `xsd:date(...)`
|| `xsd:time(...)`
|| `xsd:yearmonth(...)`
|| `xsd:year(...)`
RDF | xmlns:xsd="http://www.w3.org/2001/XMLSchema#dateTime"
|| xmlns:xsd="http://www.w3.org/2001/XMLSchema#date"
|| xmlns:xsd="http://www.w3.org/2001/XMLSchema#time"
|| xmlns:xsd="http://www.w3.org/2001/XMLSchema#gYearMonth"
|| xmlns:xsd="http://www.w3.org/2001/XMLSchema#gYear"


Note that library uses same native format to manipulate with:
* _date_ represent the beginning of each date, implicitly set time to '00:00:00'
* _time_ represents an instant of time that recurs every day. It is counted as number of microseconds from beginning of epoch 1970-01-01.
* _gYearMonth_ represents a specific Gregorian month in a specific year, implicitly set time to beginning of month first day at '00:00:00'.
* _gYear_ represents a Gregorian calendar year, implicitly set time to beginning of year first month, first day at '00:00:00'.


### recursive date time

XSD defines set of recursive data-types. They are not yet natively supported by library and translated as strings

Lang | Data type
---  | ---
Erlang | `-type binary().`
Semantic | `?XSD_MONTHDAY`
|| `?XSD_MONTH`
|| `?XSD_DAY`
Elastic | Not supported yet
Datalog | Not Supported yet
RDF | xmlns:xsd="http://www.w3.org/2001/XMLSchema#gMonthDay"
|| xmlns:xsd="http://www.w3.org/2001/XMLSchema#gMonth"
|| xmlns:xsd="http://www.w3.org/2001/XMLSchema#gDay"


### geo

The geographical coordinates are hashed using GeoHash, ensuring 3.7cm x 1.8cm precision. The hashing is only applied to `georss:point`. Other formats are not supported yet.

Lang | Data type
---  | ---
Erlang | `-type #{<<"type">> => binary(), <<"coordinates">> => _}.`
Semantic | `?GEORSS_JSON`
Elastic | `geo_shape`
Datalog | `georss:json(...)`
RDF | xmlns:xsd="http://www.georss.org/georss/json"


