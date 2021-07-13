# Apache Spark SQL connector for Google BigQuery (Beta)

<!--- TODO(#2): split out into more documents. -->

The connector supports reading [Google BigQuery](https://cloud.google.com/bigquery/) tables into Spark's DataFrames, and writing DataFrames back into BigQuery.
This is done by using the [Spark SQL Data Source API](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources) to communicate with BigQuery.

## Beta Disclaimer

The BigQuery Storage API and this connector are in Beta and are subject to change.

Changes may include, but are not limited to:
* Type conversion
* Partitioning
* Parameters

Breaking changes will be restricted to major and minor versions.

## BigQuery Storage API
The [Storage API](https://cloud.google.com/bigquery/docs/reference/storage) streams data in parallel directly from BigQuery via gRPC without using Google Cloud Storage as an intermediary.

It has a number of advantages over using the previous export-based read flow that should generally lead to better read performance:

### Direct Streaming

It does not leave any temporary files in Google Cloud Storage. Rows are read directly from BigQuery servers using the Arrow or Avro wire formats.

### Filtering

The new API allows column and predicate filtering to only read the data you are interested in.

#### Column Filtering

Since BigQuery is [backed by a columnar datastore](https://cloud.google.com/blog/big-data/2016/04/inside-capacitor-bigquerys-next-generation-columnar-storage-format), it can efficiently stream data without reading all columns.

#### Predicate Filtering

The Storage API supports arbitrary pushdown of predicate filters. Connector version 0.8.0-beta and above support pushdown of arbitrary filters to Bigquery.

There is a known issue in Spark that does not allow pushdown of filters on nested fields. For example - filters like `address.city = "Sunnyvale"` will not get pushdown to Bigquery.

### Dynamic Sharding

The API rebalances records between readers until they all complete. This means that all Map phases will finish nearly concurrently. See this blog article on [how dynamic sharding is similarly used in Google Cloud Dataflow](https://cloud.google.com/blog/big-data/2016/05/no-shard-left-behind-dynamic-work-rebalancing-in-google-cloud-dataflow).

See [Configuring Partitioning](#configuring-partitioning) for more details.

## Requirements

### Enable the BigQuery Storage API

Follow [these instructions](https://cloud.google.com/bigquery/docs/reference/storage/#enabling_the_api).

### Create a Google Cloud Dataproc cluster (Optional)

If you do not have an Apache Spark environment you can create a Cloud Dataproc cluster with pre-configured auth. The following examples assume you are using Cloud Dataproc, but you can use `spark-submit` on any cluster.

Any Dataproc cluster using the API needs the 'bigquery'  or 'cloud-platform' scopes. Dataproc clusters have the 'bigquery' scope by default, so most clusters in enabled projects should work by default e.g.

```
MY_CLUSTER=...
gcloud dataproc clusters create "$MY_CLUSTER"
```

## Downloading and Using the Connector

The latest version of the connector is publicly available in the following links:

| version | Link |
| --- | --- |
| Scala 2.11 | `gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.21.1.jar` ([HTTP link](https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.11-0.21.1.jar)) |
| Scala 2.12 | `gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.21.1.jar` ([HTTP link](https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.21.1.jar)) |

The connector is also available from the
[Maven Central](https://repo1.maven.org/maven2/com/google/cloud/spark/)
repository. It can be used using the `--packages` option or the
`spark.jars.packages` configuration property. Use the following value

| version | Connector Artifact |
| --- | --- |
| Scala 2.11 | `com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.21.1` |
| Scala 2.12 | `com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.21.1` |

If you want to keep up with the latest version of the connector the following links can be used. Notice that for production
environments where the connector version should be pinned, one of the above links should be used.

| version | Link |
| --- | --- |
| Scala 2.11 | `gs://spark-lib/bigquery/spark-bigquery-latest_2.11.jar` ([HTTP link](https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest_2.11.jar)) |
| Scala 2.12 | `gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar` ([HTTP link](https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest_2.12.jar)) |


## Hello World Example

You can run a simple PySpark wordcount against the API without compilation by running

**Dataproc image 1.5 and above**

```
gcloud dataproc jobs submit pyspark --cluster "$MY_CLUSTER" \
  --jars gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
  examples/python/shakespeare.py
```

**Dataproc image 1.4 and below**

```
gcloud dataproc jobs submit pyspark --cluster "$MY_CLUSTER" \
  --jars gs://spark-lib/bigquery/spark-bigquery-latest_2.11.jar \
  examples/python/shakespeare.py
```

## Example Codelab ##
https://codelabs.developers.google.com/codelabs/pyspark-bigquery

## Usage

The connector uses the cross language [Spark SQL Data Source API](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources):

### Reading data from a BigQuery table

```
df = spark.read \
  .format("bigquery") \
  .load("bigquery-public-data.samples.shakespeare")
```

or the Scala only implicit API:

```
import com.google.cloud.spark.bigquery._
val df = spark.read.bigquery("bigquery-public-data.samples.shakespeare")
```

For more information, see additional code samples in
[Python](examples/python/shakespeare.py),
[Scala](connector/src/main/scala/com/google/cloud/spark/bigquery/examples/Shakespeare.scala)
and
[Java](connector/src/main/java/com/google/cloud/spark/bigquery/examples/JavaShakespeare.java).

### Reading data from a BigQuery query

The connector allows you to run any
[Standard SQL](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax)
SELECT query on BigQuery and fetch its results directly to a Spark Dataframe.
This is easily done as described in the following code sample:
```
spark.conf.set("viewsEnabled","true")
spark.conf.set("materializationDataset","<dataset>")

sql = """
  SELECT tag, COUNT(*) c
  FROM (
    SELECT SPLIT(tags, '|') tags
    FROM `bigquery-public-data.stackoverflow.posts_questions` a
    WHERE EXTRACT(YEAR FROM creation_date)>=2014
  ), UNNEST(tags) tag
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 10
  """
df = spark.read.format("bigquery").load(sql)
df.show()
```
Which yields the result
```
+----------+-------+
|       tag|      c|
+----------+-------+
|javascript|1643617|
|    python|1352904|
|      java|1218220|
|   android| 913638|
|       php| 911806|
|        c#| 905331|
|      html| 769499|
|    jquery| 608071|
|       css| 510343|
|       c++| 458938|
+----------+-------+
```
A second option is to use the `query` option like this:
```
df = spark.read.format("bigquery").option("query", sql).load()
```

Notice that the execution should be faster as only the result is transmitted
over the wire. In a similar fashion the queries can include JOINs more
efficiently then running joins on Spark or use other BigQuery features such as
[subqueries](https://cloud.google.com/bigquery/docs/reference/standard-sql/subqueries),
[BigQuery user defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions),
[wildcard tables](https://cloud.google.com/bigquery/docs/reference/standard-sql/wildcard-table-reference),
[BigQuery ML](https://cloud.google.com/bigquery-ml/docs)
and more.

In order to use this feature the following configurations MUST be set:
* `viewsEnabled` must be set to `true`.
* `materializationDataset` must be set to a dataset where the GCP user has table
  creation permission. `materializationProject` is optional.

**Note:** As mentioned in the [BigQuery documentation](https://cloud.google.com/bigquery/docs/writing-results#temporary_and_permanent_tables),
the queried tables must be in the same location as the `materializationDataset`.
Also, if the tables in the `SQL statement` are from projects other than the
`parentProject` then use the fully qualified table name i.e.
`[project].[dataset].[table]`.

**Important:** This feature is implemented by running the query on BigQuery and
saving the result into a temporary table, of which Spark will read the results
from. This may add additional costs on your BigQuery account.

### Reading From Views

The connector has a preliminary support for reading from
[BigQuery views](https://cloud.google.com/bigquery/docs/views-intro). Please
note there are a few caveats:

* BigQuery views are not materialized by default, which means that the connector
  needs to materialize them before it can read them. This process affects the
  read performance, even before running any `collect()` or `count()` action.
* The materialization process can also incur additional costs to your BigQuery
  bill.
* By default, the materialized views are created in the same project and
  dataset. Those can be configured by the optional `viewMaterializationProject`
  and `viewMaterializationDataset` options, respectively. These options can also
  be globally set by calling `spark.conf.set(...)` before reading the views.
* Reading from views is **disabled** by default. In order to enable it,
  either set the viewsEnabled option when reading the specific view
  (`.option("viewsEnabled", "true")`) or set it globally by calling
  `spark.conf.set("viewsEnabled", "true")`.
* As mentioned in the [BigQuery documentation](https://cloud.google.com/bigquery/docs/writing-results#temporary_and_permanent_tables),
the `viewMaterializationDataset` should be in same location as the view.

### Writing data to BigQuery

Writing a DataFrame to BigQuery is done in a similar manner. Notice that the process writes the data first to GCS and then loads it to BigQuery, a GCS bucket must be configured to indicate the temporary data location.

```
df.write \
  .format("bigquery") \
  .option("temporaryGcsBucket","some-bucket") \
  .save("dataset.table")
```

The data is temporarily stored using the [Apache parquet](https://parquet.apache.org/) format. An alternative format is [Apache ORC](https://orc.apache.org/).

The GCS bucket and the format can also be set globally using Spark"s RuntimeConfig like this:
```
spark.conf.set("temporaryGcsBucket","some-bucket")
df.write \
  .format("bigquery") \
  .save("dataset.table")
```

When streaming a DataFrame to BigQuery, each batch is written in the same manner as a non-streaming DataFrame.
Note that a HDFS compatible
[checkpoint location](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing)
(eg: `path/to/HDFS/dir` or `gs://checkpoint-bucket/checkpointDir`) must be specified.

```
df.writeStream \
  .format("bigquery") \
  .option("temporaryGcsBucket","some-bucket") \
  .option("checkpointLocation", "some-location") \
  .option("table", "dataset.table")
```

**Important:** The connector does not configure the GCS connector, in order to avoid conflict with another GCS connector, if exists. In order to use the write capabilities of the connector, please configure the GCS connector on your cluster as explained [here](https://github.com/GoogleCloudPlatform/bigdata-interop/tree/master/gcs).

### Properties

The API Supports a number of options to configure the read

<!--- TODO(#2): Convert to markdown -->
<table>
  <tr valign="top">
   <th>Property</th>
   <th>Meaning</th>
   <th>Usage</th>
  </tr>
  <tr valign="top">
   <td><code>table</code>
   </td>
   <td>The BigQuery table in the format <code>[[project:]dataset.]table</code>.
       It is recommended to use the <code>path</code> parameter of
       <code>load()</code>/<code>save()</code> instead. This option has been
       deprecated and will be removed in a future version.
       <br/><strong>(Deprecated)</strong>
   </td>
   <td>Read/Write</td>
  </tr>
  <tr valign="top">
   <td><code>dataset</code>
   </td>
   <td>The dataset containing the table. This option should be used with
   standard table and views, but not when loading query results.
       <br/>(Optional unless omitted in <code>table</code>)
   </td>
   <td>Read/Write</td>
  </tr>
  <tr valign="top">
   <td><code>project</code>
   </td>
   <td>The Google Cloud Project ID of the table. This option should be used with
   standard table and views, but not when loading query results.
       <br/>(Optional. Defaults to the project of the Service Account being used)
   </td>
   <td>Read/Write</td>
  </tr>
  <tr valign="top">
   <td><code>parentProject</code>
   </td>
   <td>The Google Cloud Project ID of the table to bill for the export.
       <br/>(Optional. Defaults to the project of the Service Account being used)
   </td>
   <td>Read/Write</td>
  </tr>
  <tr valign="top">
   <td><code>maxParallelism</code>
   </td>
   <td>The maximal number of partitions to split the data into. Actual number
       may be less if BigQuery deems the data small enough. If there are not
       enough executors to schedule a reader per partition, some partitions may
       be empty.
       <br/><b>Important:</b> The old parameter (<code>parallelism</code>) is
            still supported but in deprecated mode. It will ve removed in
            version 1.0 of the connector.
       <br/>(Optional. Defaults to one partition per 400MB. See
       <a href="#configuring-partitioning">Configuring Partitioning</a>.)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>viewsEnabled</code>
   </td>
   <td>Enables the connector to read from views and not only tables. Please read
       the <a href="#reading-from-views">relevant section</a> before activating
       this option.
       <br/>(Optional. Defaults to <code>false</code>)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>materializationProject</code>
   </td>
   <td>The project id where the materialized view is going to be created
       <br/>(Optional. Defaults to view's project id)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>materializationDataset</code>
   </td>
   <td>The dataset where the materialized view is going to be created. This
   dataset should be in same location as the view or the queried tables.
       <br/>(Optional. Defaults to view's dataset)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>materializationExpirationTimeInMinutes</code>
   </td>
   <td>The expiration time of the temporary table holding the materialized data
       of a view or a query, in minutes. Notice that the connector may re-use
       the temporary table due to the use of local cache and in order to reduce
       BigQuery computation, so very low values may cause errors. The value must
       be a positive integer.
       <br/>(Optional. Defaults to 1440, or 24 hours)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>readDataFormat</code>
   </td>
   <td>Data Format for reading from BigQuery. Options : <code>ARROW</code>, <code>AVRO</code>
	Unsupported Arrow filters are not pushed down and results are filtered later by Spark.
	(Currently Arrow does not suport disjunction across columns).
       <br/>(Optional. Defaults to <code>ARROW</code>)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>optimizedEmptyProjection</code>
   </td>
   <td>The connector uses an optimized empty projection (select without any
       columns) logic, used for <code>count()</code> execution. This logic takes
       the data directly from the table metadata or performs a much efficient
       `SELECT COUNT(*) WHERE...` in case there is a filter. You can cancel the
       use of this logic byt setting this option to <code>false</code>.
       <br/>(Optional, defaults to <code>true</code>)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
   <td><code>pushAllFilters</code>
   </td>
   <td>If set to <code>true</code>, the connector pushes all the filters Spark can delegate
       to BigQuery Storage API. This reduces amount of data that needs to be sent from
       BigQuery Storage API servers to Spark clients.
       <br/>(Optional, defaults to <code>true</code>)
   </td>
   <td>Read</td>
  </tr>
  <tr valign="top">
     <td><code>createDisposition</code>
      </td>
      <td>Specifies whether the job is allowed to create new tables. The permitted
          values are:
          <ul>
            <li><code>CREATE_IF_NEEDED</code> - Configures the job to create the
                table if it does not exist.</li>
            <li><code>CREATE_NEVER</code> - Configures the job to fail if the
                table does not exist.</li>
          </ul>
          This option takes place only in case Spark has decided to write data
          to the table based on the SaveMode.
         <br/>(Optional. Default to CREATE_IF_NEEDED).
      </td>
      <td>Write</td>
   </tr>

  <tr valign="top">
   <td><code>temporaryGcsBucket</code>
   </td>
   <td>The GCS bucket that temporarily holds the data before it is loaded to
       BigQuery. Required unless set in the Spark configuration
       (<code>spark.conf.set(...)</code>).
   </td>
   <td>Write</td>
  </tr>
  <tr valign="top">
   <td><code>persistentGcsBucket</code>
   </td>
   <td>The GCS bucket that holds the data before it is loaded to
       BigQuery. If informed, the data won't be deleted after write data
       into BigQuery.
   </td>
   <td>Write</td>
  </tr>
  <tr valign="top">
   <td><code>persistentGcsPath</code>
   </td>
   <td>The GCS path that holds the data before it is loaded to
       BigQuery. Used only with <code>persistentGcsBucket</code>.
   </td>
   <td>Write</td>
  </tr>
  <tr valign="top">
   <td><code>intermediateFormat</code>
   </td>
   <td>The format of the data before it is loaded to BigQuery, values can be
       either "parquet","orc" or "avro". In order to use the Avro format, the
       spark-avro package must be added in runtime.
       <br/>(Optional. Defaults to <code>parquet</code>). On write only.
   </td>
   <td>Write</td>
  </tr>
  <tr valign="top">
   <td><code>datePartition</code>
   </td>
   <td>The date partition the data is going to be written to. Should be given in the
       format <code>YYYYMMDD</code>. Can be used to overwrite the data of a single
       partition, like this: <code><br/>df.write.format("bigquery")
       <br/>&nbsp;&nbsp;.option("datePartition", "YYYYMMDD")
       <br/>&nbsp;&nbsp;.mode("overwrite")
       <br/>&nbsp;&nbsp;.save("table")</code>
       <br/>(Optional). On write only.
   </td>
   <td>Write</td>
  </tr>
  <tr valign="top">
     <td><code>partitionField</code>
     </td>
     <td>If field is specified together with `partitionType`, the table is partitioned by this field.
         The field must be a top-level TIMESTAMP or DATE field. Its mode must be <strong>NULLABLE</strong>
         or <strong>REQUIRED</strong>.
         If the option is not set for a partitioned table, then the table will be partitioned by pseudo
         column, referenced via either<code>'_PARTITIONTIME' as TIMESTAMP</code> type, or
         <code>'_PARTITIONDATE' as DATE</code> type.
         <br/>(Optional).
     </td>
     <td>Write</td>
    </tr>
   <tr valign="top">
    <td><code>partitionExpirationMs</code>
     </td>
     <td>Number of milliseconds for which to keep the storage for partitions in the table.
         The storage in a partition will have an expiration time of its partition time plus this value.
        <br/>(Optional).
     </td>
     <td>Write</td>
   </tr>
   <tr valign="top">
       <td><code>partitionType</code>
        </td>
        <td>The only type supported is DAY, which will generate one partition per day. This option is <b>mandatory</b>
            for a target table to be partitioned.
            <br/>(Optional. Defaults to DAY if PartitionField is specified).
        </td>
        <td>Write</td>
     </tr>
    <tr valign="top">
           <td><code>clusteredFields</code>
            </td>
            <td>Comma separated list of non-repeated, top level columns. Clustering is only supported for partitioned tables
               <br/>(Optional).
            </td>
            <td>Write</td>
         </tr>
   <tr valign="top">
       <td><code>allowFieldAddition</code>
        </td>
        <td>Adds the <a href="https://googleapis.dev/java/google-cloud-clients/latest/com/google/cloud/bigquery/JobInfo.SchemaUpdateOption.html#ALLOW_FIELD_ADDITION" target="_blank">ALLOW_FIELD_ADDITION</a>
            SchemaUpdateOption to the BigQuery LoadJob. Allowed vales are <code>true</code> and <code>false</code>.
           <br/>(Optional. Default to <code>false</code>).
        </td>
        <td>Write</td>
     </tr>
   <tr valign="top">
       <td><code>allowFieldRelaxation</code>
        </td>
        <td>Adds the <a href="https://googleapis.dev/java/google-cloud-clients/latest/com/google/cloud/bigquery/JobInfo.SchemaUpdateOption.html#ALLOW_FIELD_RELAXATION" target="_blank">ALLOW_FIELD_RELAXATION</a>
            SchemaUpdateOption to the BigQuery LoadJob. Allowed vales are <code>true</code> and <code>false</code>.
           <br/>(Optional. Default to <code>false</code>).
        </td>
        <td>Write</td>
     </tr>
</table>

Options can also be set outside of the code, using the `--conf` parameter of `spark-submit` or `--properties` parameter
of the `gcloud dataproc submit spark`. In order to use this, prepend the prefix `spark.datasource.bigquery.` to any of
the options, for example `spark.conf.set("temporaryGcsBucket", "some-bucket")` can also be set as
`--conf spark.datasource.bigquery.temporaryGcsBucket=some-bucket`.

### Data types

With the exception of `DATETIME` and `TIME` all BigQuery data types directed map into the corresponding Spark SQL data type. Here are all of the mappings:

<!--- TODO(#2): Convert to markdown -->
<table>
  <tr valign="top">
   <td><strong>BigQuery Standard SQL Data Type </strong>
   </td>
   <td><strong>Spark SQL</strong>
<p>
<strong>Data Type</strong>
   </td>
   <td><strong>Notes</strong>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>BOOL</code></strong>
   </td>
   <td><strong><code>BooleanType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>INT64</code></strong>
   </td>
   <td><strong><code>LongType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>FLOAT64</code></strong>
   </td>
   <td><strong><code>DoubleType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>NUMERIC</code></strong>
   </td>
   <td><strong><code>DecimalType</code></strong>
   </td>
   <td>
     This preserves <code>NUMERIC</code>'s full 38 digits of precision and 9 digits of scope.
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>STRING</code></strong>
   </td>
   <td><strong><code>StringType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>BYTES</code></strong>
   </td>
   <td><strong><code>BinaryType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>STRUCT</code></strong>
   </td>
   <td><strong><code>StructType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>ARRAY</code></strong>
   </td>
   <td><strong><code>ArrayType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>TIMESTAMP</code></strong>
   </td>
   <td><strong><code>TimestampType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>DATE</code></strong>
   </td>
   <td><strong><code>DateType</code></strong>
   </td>
   <td>
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>DATETIME</code></strong>
   </td>
   <td><strong><code>StringType</code></strong>
   </td>
   <td>Spark has no DATETIME type. Casting to TIMESTAMP uses a configured TimeZone, which defaults to the local timezone (UTC in GCE / Dataproc).
<p>
We are considering adding an optional TimeZone property to allow automatically  converting to TimeStamp, this would be consistent with Spark's handling of CSV/JSON (except they always try to convert when inferring schema, and default to the local timezone)
   </td>
  </tr>
  <tr valign="top">
   <td><strong><code>TIME</code></strong>
   </td>
   <td><strong><code>LongType</code></strong>
   </td>
   <td>Spark has no TIME type. The generated longs, which indicate <a href="https://avro.apache.org/docs/1.8.0/spec.html#Time+%2528microsecond+precision%2529">microseconds since midnight</a> can be safely cast to TimestampType, but this causes the date to be inferred as the current day. Thus times are left as longs and user can cast if they like.
<p>
When casting to Timestamp TIME have the same TimeZone issues as DATETIME
   </td>
  </tr>
</table>

#### Spark ML Data Types Support

The Spark ML [Vector](https://spark.apache.org/docs/2.4.5/api/python/pyspark.ml.html#pyspark.ml.linalg.Vector) and
[Matrix](https://spark.apache.org/docs/2.4.5/api/python/pyspark.ml.html#pyspark.ml.linalg.Matrix) are supported,
including their dense and sparse versions. The data is saved as a BigQuery RECORD. Notice that a suffix is added to
the field's description which includes the spark type of the field.

In order to write those types to BigQuery, use the ORC or Avro intermediate format, and have them as column of the
Row (i.e. not a field in a struct).

### Filtering

The connector automatically computes column and pushdown filters the DataFrame's `SELECT` statement e.g.

```
spark.read.bigquery("bigquery-public-data:samples.shakespeare")
  .select("word")
  .where("word = 'Hamlet' or word = 'Claudius'")
  .collect()
```


filters to the column `word`  and pushed down the predicate filter `word = 'hamlet' or word = 'Claudius'`.

If you do not wish to make multiple read requests to BigQuery, you can cache the DataFrame before filtering e.g.:

```
val cachedDF = spark.read.bigquery("bigquery-public-data:samples.shakespeare").cache()
val rows = cachedDF.select("word")
  .where("word = 'Hamlet'")
  .collect()
// All of the table was cached and this doesn't require an API call
val otherRows = cachedDF.select("word_count")
  .where("word = 'Romeo'")
  .collect()
```

You can also manually specify the `filter` option, which will override automatic pushdown and Spark will do the rest of the filtering in the client.

### Partitioned Tables

The pseudo columns \_PARTITIONDATE and \_PARTITIONTIME are not part of the table schema. Therefore in order to query by the partitions of [partitioned tables](https://cloud.google.com/bigquery/docs/partitioned-tables) do not use the where() method shown above. Instead, add a filter option in the following manner:

```
val df = spark.read.format("bigquery")
  .option("filter", "_PARTITIONDATE > '2019-01-01'")
  ...
  .load(TABLE)
```

### Configuring Partitioning

By default the connector creates one partition per 400MB in the table being read (before filtering). This should roughly correspond to the maximum number of readers supported by the BigQuery Storage API.
This can be configured explicitly with the <code>[parallelism](#properties)</code> property. BigQuery may limit the number of partitions based on server constraints.

## Using in Jupyter Notebooks

The connector can be used in [Jupyter notebooks](https://jupyter.org/) even if
it is not installed on the Spark cluster. It can be added as an external jar in
using the following code:

**Python:**
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder\
  .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.21.1")\
  .getOrCreate()
df = spark.read.format("bigquery")\
  .load("dataset.table")
```

**Scala:**
```python
val spark = SparkSession.builder
  .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.21.1")
  .getOrCreate()
val df = spark.read.format("bigquery")
  .load("dataset.table")
```

In case Spark cluster is using Scala 2.12 (it's optional for Spark 2.4.x,
mandatory in 3.0.x), then the relevant package is
com.google.cloud.spark:spark-bigquery-with-dependencies_**2.12**:0.21.1. In
order to know which Scala version is used, please run the following code:

**Python:**
```python
spark.sparkContext._jvm.scala.util.Properties.versionString()
```

**Scala:**
```python
scala.util.Properties.versionString
```
## Compiling against the connector

Unless you wish to use the implicit Scala API `spark.read.bigquery("TABLE_ID")`, there is no need to compile against the connector.

To include the connector in your project:

### Maven

```xml
<dependency>
  <groupId>com.google.cloud.spark</groupId>
  <artifactId>spark-bigquery-with-dependencies_${scala.version}</artifactId>
  <version>0.21.1</version>
</dependency>
```

### SBT

```sbt
libraryDependencies += "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.21.1"
```

## FAQ

### What is the Pricing for the Storage API?

See the [BigQuery pricing documentation](https://cloud.google.com/bigquery/pricing#storage-api).

### I have very few partitions

You can manually set the number of partitions with the `maxParallelism` property. BigQuery may provide fewer partitions than you ask for. See [Configuring Partitioning](#configuring-partitioning).

You can also always repartition after reading in Spark.

### How do I authenticate outside GCE / Dataproc?

Use a service account JSON key and `GOOGLE_APPLICATION_CREDENTIALS` as described [here](https://cloud.google.com/docs/authentication/getting-started).

Credentials can also be provided explicitly either as a parameter or from Spark runtime configuration.
It can be passed in as a base64-encoded string directly, or a file path that contains the credentials (but not both).

Example:
```
spark.read.format("bigquery").option("credentials", "<SERVICE_ACCOUNT_JSON_IN_BASE64>")
```
or
```
spark.conf.set("credentials", "<SERVICE_ACCOUNT_JSON_IN_BASE64>")
```

Alternatively, specify the credentials file name.

```
spark.read.format("bigquery").option("credentialsFile", "</path/to/key/file>")
```
or
```
spark.conf.set("credentialsFile", "</path/to/key/file>")
```

Another alternative to passing the credentials, is to pass the access token used for authenticating
the API calls to the Google Cloud Platform APIs. You can get the access token by running
`gcloud auth application-default print-access-token`.

```
spark.read.format("bigquery").option("gcpAccessToken", "<acccess-token>")
```
or
```
spark.conf.set("gcpAccessToken", "<access-token>")
```
