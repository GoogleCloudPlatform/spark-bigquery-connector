/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigquery.connector.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * Some JDKs implement the ReadableByteChannel that wraps an input stream with a channel that will
 * intercept and reraise interrupted exception. Normally, this is a good thing, however with the
 * connector use-case this triggers a memory leak in the Arrow library (present as of 4.0).
 *
 * <p>So this is a temporary hack to ensure non-blocking behavior to avoid the memory leak. This
 * should be fine because the underlying stream will be interrupted appropriately and only along
 * full message boundaries, which should be sufficient for our use-cases.
 */
public class NonInterruptibleBlockingBytesChannel implements ReadableByteChannel {
  private static final int TRANSFER_SIZE = 4096;
  private final InputStream is;
  private boolean closed = false;
  private byte[] transferBuffer = new byte[TRANSFER_SIZE];

  public NonInterruptibleBlockingBytesChannel(InputStream is) {
    this.is = is;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int len = dst.remaining();
    int totalRead = 0;
    int bytesRead = 0;

    while (totalRead < len) {
      int bytesToRead = Math.min((len - totalRead), TRANSFER_SIZE);
      bytesRead = is.read(transferBuffer, 0, bytesToRead);
      if (bytesRead < 0) {
        break;
      } else {
        totalRead += bytesRead;
      }
      dst.put(transferBuffer, 0, bytesRead);
    }
    if ((bytesRead < 0) && (totalRead == 0)) {
      return -1;
    }

    return totalRead;
  }

  @Override
  public boolean isOpen() {
    return !closed;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    is.close();
  }
}
