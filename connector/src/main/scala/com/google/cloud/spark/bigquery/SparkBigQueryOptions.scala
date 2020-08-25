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

import java.io.{ByteArrayInputStream, FileInputStream}
import java.security.Policy.Parameters
import java.time.format.{DateTimeFormatter, DateTimeParseException}

import com.google.api.client.util.Base64
import com.google.auth.Credentials
import com.google.auth.oauth2.{AccessToken, GoogleCredentials}
import com.google.cloud.bigquery.JobInfo.CreateDisposition
import com.google.cloud.bigquery.storage.v1.DataFormat
import com.google.cloud.bigquery.{BigQueryOptions, FormatOptions, JobInfo, TableId}
import com.google.common.collect.ImmutableList
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.Properties

/** Options for defining {@link BigQueryRelation}s */
  case class SparkBigQueryOptions(
    tableId: TableId,
    parentProject: String,
    credentials: Option[String] = None,
    credentialsFile: Option[String] = None,
    filter: Option[String] = None,
    schema: Option[StructType] = None,
    maxParallelism: Option[Int] = None,
    temporaryGcsBucket: Option[String] = None,
    persistentGcsBucket: Option[String] = None,
    persistentGcsPath: Option[String] = None,
    intermediateFormat: IntermediateFormat = SparkBigQueryOptions.DefaultIntermediateFormat,
    readDataFormat: DataFormat = SparkBigQueryOptions.DefaultReadDataFormat,
    combinePushedDownFilters: Boolean = true,
    viewsEnabled: Boolean = false,
    materializationProject: Option[String] = None,
    materializationDataset: Option[String] = None,
    datePartition: Option[String] = None,
    partitionField: Option[String] = None,
    partitionExpirationMs: Option[Long] = None,
    partitionRequireFilter: Option[Boolean] = None,
    partitionType: Option[String] = None,
    clusteredFields: Option[Array[String]] = None,
    createDisposition: Option[CreateDisposition] = None,
    optimizedEmptyProjection: Boolean = true,
    accessToken: Option[String] = None,
    loadSchemaUpdateOptions: java.util.List[JobInfo.SchemaUpdateOption] = ImmutableList.of(),
    realTime: Boolean = false,
    ignoreInsertIds: Boolean = true,
    viewExpirationTimeInHours: Int = 24,
    maxReadRowsRetries: Int = 3
  ) {

  def createCredentials: Option[Credentials] =
    (accessToken, credentials, credentialsFile) match {
      case (Some(accToken), _, _) =>
        Some(GoogleCredentials.create(new AccessToken(accToken, null)))
      case (_, Some(key), None) =>
        Some(GoogleCredentials.fromStream(new ByteArrayInputStream(Base64.decodeBase64(key))))
      case (_, None, Some(file)) =>
        Some(GoogleCredentials.fromStream(new FileInputStream(file)))
      case (_, None, None) =>
        None
      case (_, Some(_), Some(_)) =>
        throw new IllegalArgumentException("Only one of credentials or credentialsFile can be" +
          " specified in the options.")
    }
}

/** Resolvers for {@link SparkBigQueryOptions} */
object SparkBigQueryOptions {

  val GcsConfigCredentialsFileProperty = "google.cloud.auth.service.account.json.keyfile"
  val GcsConfigProjectIdProperty = "fs.gs.project.id"

  val IntermediateFormatOption = "intermediateFormat"
  val ReadDataFormatOption = "readDataFormat"
  val ViewsEnabledOption = "viewsEnabled"

  val DefaultReadDataFormat: DataFormat = DataFormat.ARROW
  val DefaultFormat: String = "parquet"
  val DefaultIntermediateFormat: IntermediateFormat =
    IntermediateFormat(DefaultFormat, FormatOptions.parquet())
  private val PermittedReadDataFormats = Set(DataFormat.ARROW.toString, DataFormat.AVRO.toString)

  val GcsAccessToken = "gcpAccessToken"

  val ConfPrefix = "spark.datasource.bigquery."

  private[bigquery] def normalizeAllConf(allConf: Map[String, String]): Map[String, String] = {
    allConf ++ allConf
      .filterKeys(_.startsWith(ConfPrefix))
      .map { case(key, value) => (key.substring(ConfPrefix.length), value) }
  }

  def apply(
             parameters: Map[String, String],
             allConf: Map[String, String],
             hadoopConf: Configuration,
             sqlConf: SQLConf,
             sparkVersion: String,
             schema: Option[StructType])
  : SparkBigQueryOptions = {
    val normalizedAllConf = normalizeAllConf(allConf)

    val tableParam = getRequiredOption(parameters, "table", () => getOption(parameters, "path"))
    val datasetParam = getOption(parameters, "dataset")
    val projectParam = getOption(parameters, "project")
      .orElse(Option(hadoopConf.get(GcsConfigProjectIdProperty)))
    val credsParam = getAnyOption(normalizedAllConf, parameters, "credentials")
    val credsFileParam = getAnyOption(normalizedAllConf, parameters, "credentialsFile")
      .orElse(Option(hadoopConf.get(GcsConfigCredentialsFileProperty)))
    val datePartitionParam = getOption(parameters, "datePartition")
    datePartitionParam.foreach(validateDateFormat(_, "datePartition"))
    val tableId = BigQueryUtil.parseTableId(tableParam, datasetParam, projectParam, datePartitionParam)
    val parentProject = getRequiredOption(parameters, "parentProject",
      defaultBilledProject)
    val filter = getOption(parameters, "filter")
    val maxParallelism = getOptionFromMultipleParams(
      parameters, Seq("maxParallelism", "parallelism"))
      .map(_.toInt)
    val temporaryGcsBucket = getAnyOption(normalizedAllConf, parameters, "temporaryGcsBucket")
    val persistentGcsBucket = getAnyOption(normalizedAllConf, parameters, "persistentGcsBucket")
    val persistentGcsPath = getOption(parameters, "persistentGcsPath")

    val intermediateFormat = IntermediateFormat(
      getAnyOption(normalizedAllConf, parameters, IntermediateFormatOption)
        .map(s => s.toLowerCase())
        .getOrElse(DefaultFormat), sparkVersion, sqlConf)

    val readDataFormatParam = getAnyOption(normalizedAllConf, parameters, ReadDataFormatOption)
      .map(s => s.toUpperCase())
      .getOrElse(DefaultReadDataFormat.toString)
    if (!PermittedReadDataFormats.contains(readDataFormatParam)) {
      throw new IllegalArgumentException(
        s"""Data read format '${readDataFormatParam.toString}' is not supported.
           |Supported formats are '${PermittedReadDataFormats.mkString(",")}'"""
          .stripMargin.replace('\n', ' '))
    }
    val readDataFormat = DataFormat.valueOf(readDataFormatParam)
    val combinePushedDownFilters = getAnyBooleanOption(
      normalizedAllConf, parameters, "combinePushedDownFilters", true)
    val viewsEnabled = getAnyBooleanOption(
      normalizedAllConf, parameters, ViewsEnabledOption, false)
    val materializationProject =
      getAnyOption(normalizedAllConf, parameters,
        Seq("materializationProject", "viewMaterializationProject"))
    val materializationDataset =
      getAnyOption(normalizedAllConf, parameters,
        Seq("materializationDataset", "viewMaterializationDataset"))

    val partitionField = getOption(parameters, "partitionField")
    val partitionExpirationMs = getOption(parameters, "partitionExpirationMs").map(_.toLong)
    val partitionRequireFilter = getOption(parameters, "partitionRequireFilter").map(_.toBoolean)
    val partitionType = getOption(parameters, "partitionType")
    val clusteredFields = getOption(parameters, "clusteredFields").map(_.split(","))

    val createDisposition = getOption(parameters, "createDisposition")
      .map(_.toUpperCase).map(param => CreateDisposition.valueOf(param))

    val optimizedEmptyProjection = getAnyBooleanOption(
      normalizedAllConf, parameters, "optimizedEmptyProjection", true)
    val accessToken = getAnyOption(normalizedAllConf, parameters, GcsAccessToken)

    val allowFieldAddition = getAnyBooleanOption(
      normalizedAllConf, parameters, "allowFieldAddition", false)
    val allowFieldRelaxation = getAnyBooleanOption(
      normalizedAllConf, parameters, "allowFieldRelaxation", false)
    val loadSchemaUpdateOptions = new ArrayBuffer[JobInfo.SchemaUpdateOption]
    if (allowFieldAddition) {
      loadSchemaUpdateOptions += JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION
    }
    if (allowFieldRelaxation) {
      loadSchemaUpdateOptions += JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
    }

    val realtime: Boolean = getAnyBooleanOption(
      normalizedAllConf, parameters, "realtime", false)
    val ignoreInsertIds: Boolean = getAnyBooleanOption(
      normalizedAllConf, parameters, "ignoreInsertId", true)

    SparkBigQueryOptions(tableId, parentProject, credsParam, credsFileParam,
      filter, schema, maxParallelism, temporaryGcsBucket, persistentGcsBucket,
      persistentGcsPath, intermediateFormat, readDataFormat,
      combinePushedDownFilters, viewsEnabled, materializationProject,
      materializationDataset, datePartitionParam, partitionField, partitionExpirationMs,
      partitionRequireFilter, partitionType, clusteredFields, createDisposition,
      optimizedEmptyProjection, accessToken, loadSchemaUpdateOptions.asJava,
      realtime, ignoreInsertIds)
  }

  private def validateDateFormat(date: String, optionName: String): Unit = {
    try {
      DateTimeFormatter.BASIC_ISO_DATE.parse(date)
    } catch {
      case e: DateTimeParseException => throw new IllegalArgumentException(
        s"Invalid argument for option $optionName, format is YYYYMMDD")
    }
  }

  // could not load the spark-avro data source
  private def missingAvroException(sparkVersion: String, cause: Exception) = {
    val avroPackage = if (sparkVersion >= "2.4") {
      val scalaVersion = Properties.versionNumberString
      val scalaShortVersion = scalaVersion.substring(0, scalaVersion.lastIndexOf('.'))
      s"org.apache.spark:spark-avro_$scalaShortVersion:$sparkVersion"
    } else {
      "com.databricks:spark-avro_2.11:4.0.0"
    }
    val message = s"""Avro writing is not supported, as the spark-avro has not been
                     |found. Please re-run spark with the --packages $avroPackage parameter"""
      .stripMargin.replace('\n', ' ')

    new IllegalStateException(message, cause)
  }

  private def defaultBilledProject = () =>
    Some(BigQueryOptions.getDefaultInstance.getProjectId)

  private def getRequiredOption(
                                 options: Map[String, String],
                                 name: String,
                                 fallback: () => Option[String] = () => None): String = {
    getOption(options, name, fallback)
      .getOrElse(sys.error(s"Option $name required."))
  }

  private def getOption(
                         options: Map[String, String],
                         name: String,
                         fallback: () => Option[String] = () => None): Option[String] = {
    options.get(name).orElse(fallback())
  }

  private def getOptionFromMultipleParams(
      options: Map[String, String],
      names: Seq[String],
      fallback: () => Option[String] = () => None): Option[String] = {
    names.map(getOption(options, _))
      .find(_.isDefined)
      .getOrElse(fallback())
  }

  private def getAnyOption(
      globalOptions: Map[String, String],
      options: Map[String, String],
      name: String): Option[String] =
    options.get(name).orElse(globalOptions.get(name))

  // gives the option to support old configurations as fallback
  // Used to provide backward compatibility
  private def getAnyOption(
                            globalOptions: Map[String, String],
                            options: Map[String, String],
                            names: Seq[String]): Option[String] =
    names.map(getAnyOption(globalOptions, options, _))
      .find(_.isDefined)
      .getOrElse(None)

  private def getAnyBooleanOption(globalOptions: Map[String, String],
                                  options: Map[String, String],
                                  name: String,
                                  defaultValue: Boolean): Boolean =
    getAnyOption(globalOptions, options, name)
      .map(_.toBoolean)
      .getOrElse(defaultValue)
}

  case class IntermediateFormat(dataSource: String,
                                formatOptions: FormatOptions)
  object IntermediateFormat {
    private val PermittedIntermediateFormats = Map("avro" -> FormatOptions.avro(),
      "parquet" -> FormatOptions.parquet(),
      "orc" -> FormatOptions.orc())

    def apply (format: String, sparkVersion: String, sqlConf: SQLConf) : IntermediateFormat = {
      if (!PermittedIntermediateFormats.contains(format)) {
        throw new IllegalArgumentException(
          s"""Data read format '${format}' is not supported.
             |Supported formats are '${PermittedIntermediateFormats.keys.mkString(",")}'"""
            .stripMargin.replace('\n', ' '))
      }

      if (format == "avro") {
        val dataSource = if (sparkVersion >= "2.4") {
          format
        } else {
          "com.databricks.spark.avro"
        }

        try {
          DataSource.lookupDataSource(dataSource, sqlConf)
        } catch {
          case ae: AnalysisException =>
            throw missingAvroException(sparkVersion, ae)
          case t: Throwable => throw t
        }

        return IntermediateFormat(dataSource, PermittedIntermediateFormats(format))
      }

      IntermediateFormat(format, PermittedIntermediateFormats(format))
    }

    // could not load the spark-avro data source
    private def missingAvroException(sparkVersion: String, cause: Exception) = {
      val avroPackage = if (sparkVersion >= "2.4") {
        val scalaVersion = Properties.versionNumberString
        val scalaShortVersion = scalaVersion.substring(0, scalaVersion.lastIndexOf('.'))
        s"org.apache.spark:spark-avro_$scalaShortVersion:$sparkVersion"
      } else {
        "com.databricks:spark-avro_2.11:4.0.0"
      }
      val message = s"""Avro writing is not supported, as the spark-avro has not been
                       |found. Please re-run spark with the --packages $avroPackage parameter"""
        .stripMargin.replace('\n', ' ')

      new IllegalStateException(message, cause)
    }
  }

