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

import java.io.{BufferedOutputStream, FileOutputStream}

import com.google.cloud.bigquery.Schema
import com.google.protobuf.ByteString
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.avro.{Schema => AvroSchema}
import org.apache.spark.sql.catalyst.InternalRow

class AvroBinaryIterator(bqSchema: Schema,
                         columnsInOrder: Seq[String],
                         schema: AvroSchema,
                         bytes: ByteString) extends Iterator[InternalRow] {
  // TODO(pclay): replace nulls with reusable objects

  private lazy val converter = SchemaConverters.createRowConverter(bqSchema, columnsInOrder) _
  val reader = new GenericDatumReader[GenericRecord](schema)
  val in: BinaryDecoder = new DecoderFactory().binaryDecoder(bytes.toByteArray, null)

  override def hasNext: Boolean = !in.isEnd

  override def next(): InternalRow = converter(reader.read(null, in))
}
