/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;

import java.util.Arrays;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;

/**
 * A subclass of FileTailingAdaptor that reads UTF8/ascii files and only send
 * chunks with complete lines.
 */
public class FileTailingAdaptorPreserveLines extends FileTailingAdaptor {

  private static final char SEPARATOR = '\n';

  @Override
  protected int extractRecords(ChunkReceiver eq, long buffOffsetInFile,
      byte[] buf) throws InterruptedException {
    int lastNewLineOffset = 0;
    for (int i = buf.length - 1; i >= 0; --i) {
      if (buf[i] == SEPARATOR) {
        lastNewLineOffset = i;
        break;
      }
    }

    if (lastNewLineOffset > 0) {
      int[] offsets_i = { lastNewLineOffset };

      int bytesUsed = lastNewLineOffset + 1; // char at last
                                             // offset uses a byte
      assert bytesUsed > 0 : " shouldn't send empty events";
      ChunkImpl event = new ChunkImpl(type, toWatch.getAbsolutePath(),
          buffOffsetInFile + bytesUsed, Arrays.copyOf(buf, bytesUsed), this);

      event.setRecordOffsets(offsets_i);
      eq.add(event);

      return bytesUsed;
    } else
      return 0;
  }
}
