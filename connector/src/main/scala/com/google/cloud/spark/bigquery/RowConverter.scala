/*
 * Copyright 2020 Google Inc. All Rights Reserved.
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

import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale

import com.google.api.services.bigquery.model.TableRow
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Convert Spark row to BigQuery TableRow object
 * @param schema
 */
case class RowConverter(schema: StructType) {
  private val DEFAULT_TYPES: Set[String] =
    Set("integer",
      "float",
      "double",
      "long",
      "boolean",
      "string")

  /**
   * Convert InternalRow to Row
   */
  private val expressionEncoder: ExpressionEncoder[Row] = RowEncoder(schema).resolveAndBind()

  def convertToRow(internalRow: InternalRow): Row = {
    expressionEncoder.fromRow(internalRow)
  }

  def convert(internalRow: InternalRow): TableRow = {
    convert(convertToRow(internalRow))
  }

  /**
   * Convert Spark SQL row to TableRow
   * @param row
   * @return
   */
  def convert(row: Row): TableRow = {
    val tableRow: TableRow = new TableRow()

    for (field <- row.schema.fields) {
      if (field.dataType.typeName == getName(Array)) {
        // Handle array
        val convertedVal: List[Any] =
          convertRepeatedField(field.dataType, row.getAs[List[Any]](field.name))
        tableRow.set(field.name, convertedVal.asJava)
      } else {
        // Handle other fields

        val convertedVal = convertField(field.dataType, row.getAs[Any](field.name))
        tableRow.set(field.name, convertedVal)
      }
    }
    tableRow
  }

  /**
   * Iterate over repeated fields and convert them
   * @param dataType
   * @param value
   * @return
   */
  private def convertRepeatedField(dataType: DataType, value: Any): List[Any] = {
    val convertedValues: ListBuffer[Any] = ListBuffer[Any]()
    val values = value.asInstanceOf[mutable.WrappedArray[Any]]
    if (values != null && values.length > 0) {
      val childFieldType = dataType.asInstanceOf[ArrayType].elementType
      for (toConvertValue: Any <- values) {
        convertedValues += convertField(childFieldType, toConvertValue)
      }
    }
    convertedValues.toList
  }

  /**
   * Handle all datatypes here
   * @param dataType
   * @param value
   * @return
   */
  private def convertField(dataType: DataType, value: Any): Any = {
    var convertedField: Any = None
    if (dataType.typeName == TimestampType.typeName) {
      convertedField = (value.asInstanceOf[Timestamp].getTime / 1000).toInt
    } else if (dataType.typeName == BinaryType.typeName) {
      convertedField = java.util.Base64.getEncoder.encode(
        value.asInstanceOf[Array[Byte]])
    } else if (dataType.typeName == getName(DecimalType)) {
      // String for now...
      convertedField = value.asInstanceOf[java.math.BigDecimal].toString
    } else if (DEFAULT_TYPES.contains(dataType.typeName)) {
      convertedField = value
    } else if (dataType.typeName == getName(StructType)) {
      convertedField = convert(value.asInstanceOf[Row])
    } else if (dataType.typeName == getName(DateType)) {

      val localDate: LocalDate = LocalDate.from(value.asInstanceOf[java.sql.Date].toInstant)
      convertedField = localDate.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    } else {
      throw new UnsupportedOperationException(
        "Unexpected BigQuery field schema type " + dataType.typeName)
    }
    convertedField
  }

  private def getName(klass: Any): String = {
    klass.getClass.getSimpleName
      .stripSuffix("$").stripSuffix("Type")
      .stripSuffix("UDT").toLowerCase(Locale.ROOT)
  }
}