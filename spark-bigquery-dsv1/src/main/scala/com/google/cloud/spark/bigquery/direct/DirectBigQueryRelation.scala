/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.direct

import java.sql.{Date, Timestamp}
import java.util.UUID
import java.util.concurrent.{Callable, TimeUnit}

import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.auth.Credentials
import com.google.cloud.bigquery.connector.common.BigQueryUtil
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions
import com.google.cloud.bigquery.storage.v1.{BigQueryReadClient, BigQueryReadSettings, CreateReadSessionRequest, DataFormat, ReadSession}
import com.google.cloud.bigquery.{BigQuery, JobInfo, QueryJobConfiguration, Schema, StandardTableDefinition, TableDefinition, TableId, TableInfo}
import com.google.cloud.spark.bigquery.{BigQueryRelation, BigQueryUtilScala, SchemaConverters, SparkBigQueryConfig, SparkBigQueryConnectorUserAgentProvider}
import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

import scala.collection.JavaConverters._

private[bigquery] class DirectBigQueryRelation(
    options: SparkBigQueryConfig,
    table: TableInfo,
    getClient: SparkBigQueryConfig => BigQueryReadClient =
         DirectBigQueryRelation.createReadClient,
    bigQueryClient: SparkBigQueryConfig => BigQuery =
         BigQueryUtilScala.createBigQuery)
    (@transient override val sqlContext: SQLContext)
    extends BigQueryRelation(options, table)(sqlContext)
        with TableScan with PrunedScan with PrunedFilteredScan {

  val tablePath: String =
    DirectBigQueryRelation.toTablePath(tableId)

  lazy val bigQuery = bigQueryClient(options)

  val topLevelFields = SchemaConverters
    .toSpark(SchemaConverters.getSchemaWithPseudoColumns(table))
    .fields
    .map(field => (field.name, field))
    .toMap

  // used to cache the table instances in order to avoid redundant queries to
  // the BigQuery service
  case class DestinationTableBuilder(querySql: String) extends Callable[TableInfo] {
    override def call(): TableInfo = createTableFromQuery(querySql)
  }
  val destinationTableCache: Cache[String, TableInfo] =
    CacheBuilder.newBuilder()
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .maximumSize(1000)
      .build()

  /**
   * Default parallelism to 1 reader per 400MB, which should be about the maximum allowed by the
   * BigQuery Storage API. The number of partitions returned may be significantly less depending
   * on a number of factors.
   */
  val DEFAULT_BYTES_PER_PARTITION = 400L * 1000 * 1000

  override val needConversion: Boolean = false
  override val sizeInBytes: Long = getNumBytes(defaultTableDefinition)
  // no added filters and with all column
  lazy val defaultTableDefinition: TableDefinition = table.getDefinition[TableDefinition]

  override def buildScan(): RDD[Row] = {
    buildScan(schema.fieldNames)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    buildScan(requiredColumns, Array())
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logInfo(
      s"""
         |Querying table $tableName, parameters sent from Spark:
         |requiredColumns=[${requiredColumns.mkString(",")}],
         |filters=[${filters.map(_.toString).mkString(",")}]"""
        .stripMargin.replace('\n', ' ').trim)
    val filter = getCompiledFilter(filters)
    val actualTable = getActualTable(requiredColumns, filter)

    logInfo(
      s"""
         |Going to read from ${BigQueryUtil.friendlyTableName(actualTable.getTableId)}
         |columns=[${requiredColumns.mkString(", ")}],
         |filter='$filter'"""
        .stripMargin.replace('\n', ' ').trim)

    if (options.isOptimizedEmptyProjection && requiredColumns.isEmpty) {
      generateEmptyRowRDD(actualTable, getActualFilter(filter))
    } else {
      if (requiredColumns.isEmpty) {
        logDebug(s"Not using optimized empty projection")
      }
      val actualTableDefinition = actualTable.getDefinition[StandardTableDefinition]
      val actualTablePath = DirectBigQueryRelation.toTablePath(actualTable.getTableId)
      val readOptions = TableReadOptions.newBuilder()
        .addAllSelectedFields(requiredColumns.toList.asJava)
        .setRowRestriction(getActualFilter(filter))
        .build()
      val requiredColumnSet = requiredColumns.toSet
      val prunedSchema = Schema.of(
	SchemaConverters.getSchemaWithPseudoColumns(actualTable).getFields().asScala
          .filter(f => requiredColumnSet.contains(f.getName)).asJava)

      val client = getClient(options)

      val maxNumPartitionsRequested = getMaxNumPartitionsRequested(actualTableDefinition)

      val readDataFormat = options.getReadDataFormat

      try {
        val session = client.createReadSession(
          CreateReadSessionRequest.newBuilder()
            .setParent(s"projects/${options.getParentProjectId}")
            .setReadSession(ReadSession.newBuilder()
              .setDataFormat(readDataFormat)
              .setReadOptions(readOptions)
              .setTable(actualTablePath
            ))
            .setMaxStreamCount(maxNumPartitionsRequested)
            .build())
        val partitions = session.getStreamsList.asScala.map(_.getName)
          .zipWithIndex.map { case (name, i) => BigQueryPartition(name, i) }
          .toArray

        logInfo(s"Created read session for table '$tableName': ${session.getName}")

        // This is spammy, but it will make it clear to users the number of partitions they got and
        // why.
        if (!maxNumPartitionsRequested.equals(partitions.length)) {
          logInfo(
            s"""Requested $maxNumPartitionsRequested max partitions, but only
               |received ${partitions.length} from the BigQuery Storage API for
               |session ${session.getName}. Notice that the number of streams in
               |actual may be lower than the requested number, depending on the
               |amount parallelism that is reasonable for the table and the
               |maximum amount of parallelism allowed by the system."""
              .stripMargin.replace('\n', ' '))
        }

        BigQueryRDD.scanTable(
          sqlContext,
          partitions.asInstanceOf[Array[Partition]],
          session,
          prunedSchema,
          requiredColumns,
          options,
          getClient,
          bigQueryClient).asInstanceOf[RDD[Row]]

      } finally {
        // scanTable returns immediately not after the actual data is read.
        client.close()
      }
    }
  }

  def  generateEmptyRowRDD(tableInfo: TableInfo, filter: String) : RDD[Row] = {
    DirectBigQueryRelation.emptyRowRDDsCreated += 1
    val numberOfRows: Long = if (filter.length == 0) {
      // no need to query the table
      tableInfo.getNumRows.longValue
    } else {
      // run a query
      val table = DirectBigQueryRelation.toSqlTableReference(tableInfo.getTableId)
      val sql = s"SELECT COUNT(*) from `$table` WHERE $filter"
      val result = bigQuery.query(QueryJobConfiguration.of(sql))
      result.iterateAll.iterator.next.get(0).getLongValue
    }
    logInfo(s"Used optimized BQ count(*) path. Count: $numberOfRows")
    sqlContext.sparkContext.range(0, numberOfRows)
      .map(_ => InternalRow.empty)
      .asInstanceOf[RDD[Row]]
  }

  def getActualTable(
      requiredColumns: Array[String],
      filtersString: String
    ): TableInfo = {
    val tableDefinition = table.getDefinition[TableDefinition]
    if(isInputTableAView) {
      // get it from the view
      val querySql = createSql(tableDefinition.getSchema, requiredColumns, filtersString)
      logDebug(s"querySql is $querySql")
      destinationTableCache.get(querySql, DestinationTableBuilder(querySql))
    } else {
      // use the default one
      table
    }
  }

  def getActualFilter(filter: String): String = {
    if(isInputTableAView){
      ""
    } else {
      filter
    }
  }

  def isInputTableAView: Boolean = {
    val tableDefinition = table.getDefinition[TableDefinition]
    val tableType = tableDefinition.getType

    val isView = options.isViewsEnabled &&
      (TableDefinition.Type.VIEW == tableType ||
        TableDefinition.Type.MATERIALIZED_VIEW == tableType)

    isView
  }

  def createTableFromQuery(querySql: String): TableInfo = {
    val destinationTable = createDestinationTable
    logDebug(s"destinationTable is $destinationTable")
    val jobInfo = JobInfo.of(
      QueryJobConfiguration
        .newBuilder(querySql)
        .setDestinationTable(destinationTable)
        .build())
    logDebug(s"running query $jobInfo")
    val job = bigQuery.create(jobInfo).waitFor()
    logDebug(s"job has finished. $job")
    if(job.getStatus.getError != null) {
      BigQueryUtil.convertAndThrow(job.getStatus.getError)
    }
    // add expiration time to the table
    val createdTable = bigQuery.getTable(destinationTable)
    val expirationTime = createdTable.getCreationTime +
      TimeUnit.MINUTES.toMillis(options.getMaterializationExpirationTimeInMinutes)
    val updatedTable = bigQuery.update(createdTable.toBuilder
      .setExpirationTime(expirationTime)
      .build())
    updatedTable
  }

  def createSql(schema: Schema, requiredColumns: Array[String], filtersString: String): String = {
    val columns = if (requiredColumns.isEmpty) {
      val sparkSchema = SchemaConverters.toSpark(schema)
      sparkSchema.map(f => s"`${f.name}`").mkString(",")
    } else {
      requiredColumns.map(c => s"`$c`").mkString(",")
    }

    val whereClause = createWhereClause(filtersString)
      .map(f => s"WHERE $f")
      .getOrElse("")

    return s"SELECT $columns FROM `$tableName` $whereClause"
  }

  // return empty if no filters are used
  def createWhereClause(filtersString: String): Option[String] = {
    BigQueryUtilScala.noneIfEmpty(filtersString)
  }

  def createDestinationTable: TableId = {
    val project = options.getMaterializationProject.orElse(tableId.getProject)
    val dataset = options.getMaterializationDataset.orElse(tableId.getDataset)
    val uuid = UUID.randomUUID()
    val name =
      s"_sbc_${uuid.getMostSignificantBits.toHexString}${uuid.getLeastSignificantBits.toHexString}"
    TableId.of(project, dataset, name)
  }

  /**
   * The theoretical number of Partitions of the returned DataFrame.
   * If the table is small the server will provide fewer readers and there will be fewer
   * partitions.
   *
   * VisibleForTesting
   */
  def getMaxNumPartitionsRequested: Int =
    getMaxNumPartitionsRequested(defaultTableDefinition)

  def getMaxNumPartitionsRequested(tableDefinition: TableDefinition): Int =
    options.getMaxParallelism
      .orElse(Math.max(
        (getNumBytes(tableDefinition) / DEFAULT_BYTES_PER_PARTITION).toInt, 1))

  def getNumBytes(tableDefinition: TableDefinition): Long = {
    val tableType = tableDefinition.getType
    if (options.isViewsEnabled &&
      (TableDefinition.Type.VIEW == tableType ||
        TableDefinition.Type.MATERIALIZED_VIEW == tableType)) {
      sqlContext.sparkSession.sessionState.conf.defaultSizeInBytes
    } else {
      tableDefinition.asInstanceOf[StandardTableDefinition].getNumBytes
    }
  }

  // VisibleForTesting
  private[bigquery] def getCompiledFilter(filters: Array[Filter]): String = {
    if (options.isCombinePushedDownFilters) {
      // new behaviour, fixing
      // https://github.com/GoogleCloudPlatform/spark-bigquery-connector/issues/74
      Seq(
        BigQueryUtilScala.toOption(options.getFilter),
        BigQueryUtilScala.noneIfEmpty(DirectBigQueryRelation.compileFilters(
          handledFilters(filters)))
      )
        .flatten
        .map(f => s"($f)")
        .mkString(" AND ")
    } else {
      // old behaviour, kept for backward compatibility
      // If a manual filter has been specified do not push down anything.
      options.getFilter.orElse {
        // TODO(pclay): Figure out why there are unhandled filters after we already listed them
        DirectBigQueryRelation.compileFilters(handledFilters(filters))
      }
    }
  }

  private def handledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter(filter => DirectBigQueryRelation.isTopLevelFieldFilterHandled(
      options.getPushAllFilters, filter, options.getReadDataFormat, topLevelFields))
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    // If a manual filter has been specified tell Spark they are all unhandled
    if (options.getFilter.isPresent) {
      return filters
    }

    val unhandled = filters.filterNot(handledFilters(filters).contains)
    logDebug(s"unhandledFilters: ${unhandled.mkString(" ")}")
    unhandled
  }
}

object DirectBigQueryRelation {

  // used for testing
  var emptyRowRDDsCreated = 0;

  def createReadClient(options: SparkBigQueryConfig): BigQueryReadClient = {
    // TODO(pmkc): investigate thread pool sizing and log spam matching
    // https://github.com/grpc/grpc-java/issues/4544 in integration tests
    val settingsBuilder = BigQueryReadSettings.defaultGrpcTransportProviderBuilder()
      .setHeaderProvider(headerProvider);
    val readSessionConfig = options.toReadSessionCreatorConfig()
    if (readSessionConfig.endpoint().isPresent()) {
      settingsBuilder.setEndpoint(readSessionConfig.endpoint().get())
    }
    var clientSettings = BigQueryReadSettings.newBuilder()
      .setTransportChannelProvider(settingsBuilder.build())
     clientSettings.setCredentialsProvider(
        new CredentialsProvider {
          override def getCredentials: Credentials = options.createCredentials
        })

    BigQueryReadClient.create(clientSettings.build)
  }

  private def headerProvider =
    FixedHeaderProvider.create("user-agent",
      new SparkBigQueryConnectorUserAgentProvider("v1").getUserAgent)

  def isTopLevelFieldFilterHandled(
      pushAllFilters: Boolean,
      filter: Filter,
      readDataFormat: DataFormat,
      fields: Map[String, StructField]): Boolean = {
    if (pushAllFilters) {
      return true
    }
    filter match {
      case EqualTo(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case GreaterThan(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case GreaterThanOrEqual(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case LessThan(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case LessThanOrEqual(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case In(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case IsNull(attr) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case IsNotNull(attr) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case And(lhs, rhs) =>
        isTopLevelFieldFilterHandled(pushAllFilters, lhs, readDataFormat, fields) &&
        isTopLevelFieldFilterHandled(pushAllFilters, rhs, readDataFormat, fields)
      case Or(lhs, rhs) =>
        readDataFormat == DataFormat.AVRO &&
        isTopLevelFieldFilterHandled(pushAllFilters, lhs, readDataFormat, fields) &&
        isTopLevelFieldFilterHandled(pushAllFilters, rhs, readDataFormat, fields)
      case Not(child) => isTopLevelFieldFilterHandled(pushAllFilters, child, readDataFormat, fields)
      case StringStartsWith(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case StringEndsWith(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case StringContains(attr, _) =>
        isFilterWithNamedFieldHandled(filter, readDataFormat, fields, attr)
      case _ => false
    }
  }

  // BigQuery Storage API does not handle RECORD/STRUCT fields at the moment
  def isFilterWithNamedFieldHandled(
      filter: Filter,
      readDataFormat: DataFormat,
      fields: Map[String, StructField],
      fieldName: String): Boolean = {
    fields.get(fieldName)
      .filter(f => (f.dataType.isInstanceOf[StructType] || f.dataType.isInstanceOf[ArrayType]))
      .map(_ => false)
      .getOrElse(isHandled(filter, readDataFormat))
  }

  def isHandled(filter: Filter, readDataFormat: DataFormat): Boolean = filter match {
    case EqualTo(_, _) => true
    // There is no direct equivalent of EqualNullSafe in Google standard SQL.
    case EqualNullSafe(_, _) => false
    case GreaterThan(_, _) => true
    case GreaterThanOrEqual(_, _) => true
    case LessThan(_, _) => true
    case LessThanOrEqual(_, _) => true
    case In(_, _) => true
    case IsNull(_) => true
    case IsNotNull(_) => true
    case And(lhs, rhs) => isHandled(lhs, readDataFormat) && isHandled(rhs, readDataFormat)
    case Or(lhs, rhs) => readDataFormat == DataFormat.AVRO &&
      isHandled(lhs, readDataFormat) && isHandled(rhs, readDataFormat)
    case Not(child) => isHandled(child, readDataFormat)
    case StringStartsWith(_, _) => true
    case StringEndsWith(_, _) => true
    case StringContains(_, _) => true
    case _ => false
  }

  // Mostly copied from JDBCRDD.scala
  def compileFilter(filter: Filter): String = filter match {
    case EqualTo(attr, value) => s"${quote(attr)} = ${compileValue(value)}"
    case GreaterThan(attr, value) => s"$attr > ${compileValue(value)}"
    case GreaterThanOrEqual(attr, value) => s"${quote(attr)} >= ${compileValue(value)}"
    case LessThan(attr, value) => s"${quote(attr)} < ${compileValue(value)}"
    case LessThanOrEqual(attr, value) => s"${quote(attr)} <= ${compileValue(value)}"
    case In(attr, values) => s"${quote(attr)} IN UNNEST(${compileValue(values)})"
    case IsNull(attr) => s"${quote(attr)} IS NULL"
    case IsNotNull(attr) => s"${quote(attr)} IS NOT NULL"
    case And(lhs, rhs) => Seq(lhs, rhs).map(compileFilter).map(p => s"($p)").mkString("(", " AND ", ")")
    case Or(lhs, rhs) => Seq(lhs, rhs).map(compileFilter).map(p => s"($p)").mkString("(", " OR ", ")")
    case Not(child) => Seq(child).map(compileFilter).map(p => s"(NOT ($p))").mkString
    case StringStartsWith(attr, value) =>
      s"${quote(attr)} LIKE '''${value.replace("'", "\\'")}%'''"
    case StringEndsWith(attr, value) =>
      s"${quote(attr)} LIKE '''%${value.replace("'", "\\'")}'''"
    case StringContains(attr, value) =>
      s"${quote(attr)} LIKE '''%${value.replace("'", "\\'")}%'''"
    case _ => throw new IllegalArgumentException(s"Invalid filter: $filter")
  }

  def compileFilters(filters: Iterable[Filter]): String = {
    filters.map(compileFilter).toSeq.sorted.mkString(" AND ")
  }

  /**
   * Converts value to SQL expression.
   */
  private def compileValue(value: Any): Any = value match {
    case null => "null"
    case stringValue: String => s"'${stringValue.replace("'", "\\'")}'"
    case timestampValue: Timestamp => "TIMESTAMP '" + timestampValue + "'"
    case dateValue: Date => "DATE '" + dateValue + "'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString("[", ", ", "]")
    case _ => value
  }

  private def quote(attr: String): String = {
    s"""`$attr`"""
  }

  def toTablePath(tableId: TableId): String =
    s"projects/${tableId.getProject}/datasets/${tableId.getDataset}/tables/${tableId.getTable}"

  def toSqlTableReference(tableId: TableId): String =
    s"${tableId.getProject}.${tableId.getDataset}.${tableId.getTable}"
}
