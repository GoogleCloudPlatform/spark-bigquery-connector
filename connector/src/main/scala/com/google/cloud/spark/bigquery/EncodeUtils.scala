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

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.google.api.services.bigquery.model.TableRow
import com.google.common.base.Utf8

/**
 * Get API object sizes
 * Ported from Beam SDK
 */
object EncodeUtils {
  private val MAPPER: ObjectMapper =
    new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)

  private def convertIntToLongNoSignExtend(v: Int) = v & 0xFFFFFFFFL

  def getLength(v: Long): Int = {
    var result = 0
    var input: Long = v
    do {
      result += 1
      input = input.>>>(7)
    } while ( {
      input != 0
    })
    result
  }

  def getEncodedElementByteSize(value: TableRow): Long = {
    val strValue = MAPPER.writeValueAsString(value)
    val size: Int = Utf8.encodedLength(strValue)
    getLength(convertIntToLongNoSignExtend(size)) + size
  }
}
