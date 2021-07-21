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

import com.google.cloud.bigquery.storage.v1.{ReadRowsRequest, ReadRowsResponse}
import com.google.cloud.spark.bigquery.direct.{ReadRowsClient, ReadRowsHelper}
import io.grpc.{Status, StatusRuntimeException}
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ReadRowsSuite extends AnyFunSuite with Matchers {

  val client = mock(classOf[ReadRowsClient])

  val request = ReadRowsRequest.newBuilder().setReadStream("test")

  test("no failures") {
    val batch1 = new MockResponsesBatch
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(10).build())
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(11).build())

    val responses = new MockReadRowsHelper(client, request, 3, Seq(batch1))
      .readRows()
      .toSeq // so we can run multiple tests

    responses.size should be(2)
    responses.map(_.getRowCount).sum should be(21)
  }

  test("retry of single failure") {
    val batch1 = new MockResponsesBatch
    batch1.addResponse(ReadRowsResponse.newBuilder().setRowCount(10).build())
    batch1.addException(new StatusRuntimeException(Status.INTERNAL.withDescription(
      "Received unexpected EOS on DATA frame from server.")))
    val batch2 = new MockResponsesBatch
    batch2.addResponse(ReadRowsResponse.newBuilder().setRowCount(11).build())

    val responses = new MockReadRowsHelper(client, request, 3, Seq(batch1, batch2))
      .readRows()
      .toSeq // so we can run multiple tests

    responses.size should be(2)
    responses.map(_.getRowCount).sum should be(21)
  }

}

class MockReadRowsHelper(
                          client: ReadRowsClient,
                          request: ReadRowsRequest.Builder,
                          maxReadRowsRetries: Int,
                          val responses: Seq[MockResponsesBatch])
  extends ReadRowsHelper(client, request, maxReadRowsRetries) {

  val responsesIterator = responses.iterator

  override private[bigquery] def fetchResponses(readRowsRequest: ReadRowsRequest.Builder) = {
    responsesIterator.next
  }

}
