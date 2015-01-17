# CSV Support in AsterixDB

## Introduction - Defining a datatype for CSV

AsterixDB supports the CSV format for both data input and query result
output. In both cases, the structure of the CSV data must be defined
using a named ADM record datatype. The CSV format, limitations, and
MIME type are defined by [RFC
4180](https://tools.ietf.org/html/rfc4180).

CSV is not as expressive as the full Asterix Data Model, meaning that
not all data which can be represented in ADM can also be represented
as CSV. So the form of this datatype is limited. First, obviously it
may not contain any nested records or lists, as CSV has no way to
represent nested data structures. All fields in the record type must
be primitive. Second, the set of supported primitive types is limited
to numerics (`int8`, `int16`, `int32`, `int64`, `float`, `double`) and
`string`.  On output, a few additional primitive types (`boolean`,
datetime types) are supported and will be represented as strings.

For the purposes of this document, we will use the following dataverse
and datatype definitions:

    drop dataverse csv if exists;
    create dataverse csv;
    use dataverse csv;

    create type "csv_type" as closed {
        "id": int32,
        "money": float,
        "name": string
    };

    create dataset "csv_set" ("csv_type") primary key "id";

Note: There is no explicit restriction against using an open datatype
for CSV purposes, and you may have optional fields in the datatype
(eg., `id: int32?`).  However, the CSV format itself is rigid, so
using either of these datatype features introduces possible failure
modes on output which will be discussed below.

## CSV Input

CSV data may be loaded into a dataset using the normal "load dataset"
mechanisms, utilizing the builtin "delimited-text" format. See
[Accessing External Data](aql/externaldata.html) for more
details. Note that comma is the default value for the "delimiter"
parameter, so it does not need to be explicitly specified.

In this case, the datatype used to interpret the CSV data is the
datatype associated with the dataset being loaded. So, to load a file
that we have stored locally on the NC into our example dataset:

    use dataverse csv;

    load dataset "csv_set" using localfs
    (("path"="127.0.0.1:///tmp/my_sample.csv"),
     ("format"="delimited-text"));

**Note:** Currently the CSV input parser only supports CSV data
without headers.

So, if the file `/tmp/my_sample.csv` contained

    1,18.50,"Peter Krabnitz"
    2,74.50,"Jesse Stevens"

then the preceding query would load it into the dataset `csv_set`.

CSV data may also be loaded from HDFS; see
[Accessing External Data](aql/externaldata.html) for details.

## CSV Output

Any query may be rendered as CSV when using AsterixDB's HTTP
interface.  To do so, there are two steps required: specify the record
type which defines the schema of your CSV, and request that Asterix
use the CSV output format.

#### Output Record Type

Background: The result of any AQL query is an unordered list of
_instances_, where each _instance_ is an instance of an AQL
datatype. When requesting CSV output, there are some restrictions on
the legal datatypes in this unordered list due to the limited
expressability of CSV:

1. Each instance must be of a record type.
2. Each instance must be of the _same_ record type.
3. The record type must conform to the content and type restrictions
mentioned in the introduction.

While it would be possible to structure your query to cast all result
instances to a given type, it is not necessary. AQL offers a built-in
feature which will automatically cast all top-level instances in the
result to a specified named ADM record type. To enable this feature,
use a `set` statement prior to the query to set the parameter
`output-record-type` to the name of an ADM type. This type must have
already been defined in the current dataverse.

For example, the following request will ensure that all result
instances are cast to the `csv_type` type declared earlier:

    use dataverse csv;
    set output-record-type "csv_type";

    for $n in dataset "csv_set" return $n;

In this case the casting is redundant since by definition every value
in `csv_set` is already of type `csv_type`. But consider a more
complex query where the result values are created by joining fields
from different underlying datasets, etc.

Two notes about `output-record-type`:

1. This feature is not strictly related to CSV; it may be used with
any output formats (in which case, any record datatype may be
specified, not subject to the limitations specified in the
introduction of this page).
2. When the CSV output format is requested, `output-record-type` is in
fact required, not optional. This is because the type is used to
determine the field names for the CSV header and to ensure that the
ordering of fields in the output is consistent (which is obviously
vital for the CSV to make any sense).

#### Request the CSV Output Format

When sending requests to the Asterix HTTP API, Asterix decides what
format to use for rendering the results based on the `Accept` HTTP
header. By default, Asterix will produce JSON output, and this can be
requested explicitly by specifying the MIME type `application/json`.
To select CSV output, set the `Accept` header on your request to the
MIME type `text/csv`. The details of how to accomplish this will of
course depend on what tools you are using to contact the HTTP API.
Here is an example from a Unix shell prompt using the command-line
utility "curl":

    curl -G -H "Accept: text/csv" "http://localhost:19002/query" --data-urlencode '
        query=use dataverse csv;
              set output-record-type "csv_type";
              for $n in dataset csv_set return $n;'

Similarly, a trivial Java program to execute the above sample query
would be:

    import java.net.HttpURLConnection;
    import java.net.URL;
    import java.net.URLEncoder;
    import java.io.BufferedReader;
    import java.io.InputStream;
    import java.io.InputStreamReader;

    public class AsterixExample {
        public static void main(String[] args) throws Exception {
            String query = "use dataverse csv; " +
                "set output-record-type \"csv_type\";" +
                "for $n in dataset csv_set return $n";
            URL asterix = new URL("http://localhost:19002/query?query=" +
                                  URLEncoder.encode(query, "UTF-8"));
            HttpURLConnection conn = (HttpURLConnection) asterix.openConnection();
            conn.setRequestProperty("Accept", "text/csv");
            BufferedReader result = new BufferedReader
                (new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = result.readLine()) != null) {
                System.out.println(line);
            }
            result.close();
        }
    }

For either of the above examples, the output would be:

    "id","money","name"
    1,18.5,"Peter Krabnitz"
    2,74.5,"Jesse Stevens"

assuming you had already run the previous examples to create the
dataverse and populate the dataset.

#### Issues with open datatypes and optional fields

As mentioned earlier, CSV is a rigid format. It cannot express records
with different numbers of fields, which ADM allows through both open
datatypes and optional fields.

If your output record type contains optional fields, this will not
result in any errors. If the output data of a query does not contain
values for an optional field, this will be represented in CSV as
`null`.

If your output record type is open, this will also not result in any
errors. If the output data of a query contains any open fields, the
corresponding rows in the resulting CSV will contain more
comma-separated values than the others. On each such row, the data
from the closed fields in the type will be output first in the normal
order, followed by the data from the open fields in an arbitrary
order.

According to RFC 4180 this is not strictly valid CSV (Section 2, rule
4, "Each line _should_ contain the same number of fields throughout
the file").  Hence it will likely not be handled consistently by all
CSV processors. Some may throw a parsing error. If you attempt to load
this data into AsterixDB later using `load dataset`, the extra fields
will be silently ignored. For this reason it is recommended that you
use only closed datatypes as output record types. AsterixDB allows to
use an open record type only to support cases where the type already
exists for other parts of your application.
