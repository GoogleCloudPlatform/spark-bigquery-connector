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
package com.google.cloud.spark.bigquery

import java.util.Optional

import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TableDefinition.Type.{MATERIALIZED_VIEW, TABLE, VIEW}
import com.google.cloud.bigquery.connector.common.{BigQueryClient, BigQueryClientModule, BigQueryUtil}
import com.google.cloud.spark.bigquery.direct.DirectBigQueryRelation
import com.google.common.collect.ImmutableMap
import com.google.inject.{Guice, Injector}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.JavaConverters._

class BigQueryRelationProvider(
                                getGuiceInjectorCreator: () => GuiceInjectorCreator)
  extends RelationProvider
    with CreatableRelationProvider
    with SchemaRelationProvider
    with DataSourceRegister
    with StreamSinkProvider {

  BigQueryUtilScala.validateScalaVersionCompatibility

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    createRelationInternal(sqlContext, parameters)
  }

  def this() = this(() => new GuiceInjectorCreator {})

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    createRelationInternal(sqlContext, parameters, schema = Some(schema))
  }

  // To allow streaming writes
  override def createSink(
                           sqlContext: SQLContext,
                           parameters: Map[String, String],
                           partitionColumns: Seq[String],
                           outputMode: OutputMode): Sink = {
    val injector = getGuiceInjectorCreator().createGuiceInjector(sqlContext, parameters)
    val opts = injector.getInstance(classOf[SparkBigQueryConfig])
    val bigQueryClient = injector.getInstance(classOf[BigQueryClient])
    BigQueryStreamingSink(
      sqlContext, parameters, partitionColumns, outputMode, opts, bigQueryClient)
  }

  protected def createRelationInternal(
                                        sqlContext: SQLContext,
                                        parameters: Map[String, String],
                                        schema: Option[StructType] = None): BigQueryRelation = {
    val injector = getGuiceInjectorCreator().createGuiceInjector(sqlContext, parameters, schema)
    val opts = injector.getInstance(classOf[SparkBigQueryConfig])
    val bigQueryClient = injector.getInstance(classOf[BigQueryClient])
    val tableInfo = bigQueryClient.getReadTable(opts.toReadTableOptions)
    val tableName = BigQueryUtil.friendlyTableName(opts.getTableId)
    val table = Option(tableInfo)
      .getOrElse(sys.error(s"Table $tableName not found"))
    table.getDefinition[TableDefinition].getType match {
      case TABLE => new DirectBigQueryRelation(opts, table)(sqlContext)
      case VIEW | MATERIALIZED_VIEW => if (opts.isViewsEnabled) {
        new DirectBigQueryRelation(opts, table)(sqlContext)
      } else {
        sys.error(
          s"""Views were not enabled. You can enable views by setting
             |'${SparkBigQueryConfig.VIEWS_ENABLED_OPTION}' to true.
             |Notice additional cost may occur."""
            .stripMargin.replace('\n', ' '))
      }
      case unsupported => throw new UnsupportedOperationException(
        s"The type of table $tableName is currently not supported: $unsupported")
    }
  }

  // to allow write support
  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               data: DataFrame): BaseRelation = {
    val injector = getGuiceInjectorCreator().createGuiceInjector(
      sqlContext, parameters, Some(data.schema))
    val options = injector.getInstance(classOf[SparkBigQueryConfig])
    val bigQueryClient = injector.getInstance(classOf[BigQueryClient])
    val tableId = options.getTableId
    val relation = BigQueryInsertableRelation(bigQueryClient, sqlContext, options)

    mode match {
      case SaveMode.Append => relation.insert(data, overwrite = false)
      case SaveMode.Overwrite => relation.insert(data, overwrite = true)
      case SaveMode.ErrorIfExists =>
        if (!relation.exists) {
          relation.insert(data, overwrite = false)
        } else {
          throw new IllegalArgumentException(
            s"""SaveMode is set to ErrorIfExists and Table
               |${BigQueryUtil.friendlyTableName(tableId)}
               |already exists. Did you want to add data to the table by setting
               |the SaveMode to Append? Example:
               |df.write.format.options.mode(SaveMode.Append).save()"""
              .stripMargin.replace('\n', ' '))
        }
      case SaveMode.Ignore =>
        if (!relation.exists) {
          relation.insert(data, overwrite = false)
        }
    }

    relation
  }

  def createSparkBigQueryConfig(sqlContext: SQLContext,
                                parameters: Map[String, String],
                                schema: Option[StructType] = None): SparkBigQueryConfig = {
    SparkBigQueryConfig.from(parameters.asJava,
      ImmutableMap.copyOf(sqlContext.getAllConfs.asJava),
      sqlContext.sparkContext.hadoopConfiguration,
      sqlContext.sparkContext.defaultParallelism,
      sqlContext.sparkSession.sessionState.conf,
      sqlContext.sparkContext.version,
      Optional.ofNullable(schema.orNull))
  }

  override def shortName: String = "bigquery"
}

// externalized to be used by tests
trait GuiceInjectorCreator {
  def createGuiceInjector(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          schema: Option[StructType] = None): Injector = {
    val spark = sqlContext.sparkSession
    val injector = Guice.createInjector(
      new BigQueryClientModule,
      new SparkBigQueryConnectorModule(
        spark, parameters.asJava, Optional.ofNullable(schema.orNull), DataSourceVersion.V1))
    injector
  }
}

// DefaultSource is required for spark.read.format("com.google.cloud.spark.bigquery")
// BigQueryRelationProvider is more consistent with the internal providers
class DefaultSource extends BigQueryRelationProvider
