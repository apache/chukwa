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
package org.apache.hadoop.chukwa;


import java.io.IOException;
import junit.framework.TestCase;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

public class ChunkImplTest extends TestCase {
  public void testVersion() {
    ChunkBuilder cb = new ChunkBuilder();
    cb.addRecord("foo".getBytes());
    cb.addRecord("bar".getBytes());
    cb.addRecord("baz".getBytes());
    Chunk c = cb.getChunk();
    DataOutputBuffer ob = new DataOutputBuffer(c.getSerializedSizeEstimate());
    try {
      c.write(ob);
      DataInputBuffer ib = new DataInputBuffer();
      ib.reset(ob.getData(), c.getSerializedSizeEstimate());
      int version = ib.readInt();
      assertEquals(version, ChunkImpl.PROTOCOL_VERSION);
    } catch (IOException e) {
      e.printStackTrace();
      fail("Should nor raise any exception");
    }
  }

  public void testWrongVersion() {
    ChunkBuilder cb = new ChunkBuilder();
    cb.addRecord("foo".getBytes());
    cb.addRecord("bar".getBytes());
    cb.addRecord("baz".getBytes());
    Chunk c = cb.getChunk();
    DataOutputBuffer ob = new DataOutputBuffer(c.getSerializedSizeEstimate());
    try {
      c.write(ob);
      DataInputBuffer ib = new DataInputBuffer();
      ib.reset(ob.getData(), c.getSerializedSizeEstimate());
      // change current chunkImpl version
      ChunkImpl.PROTOCOL_VERSION = ChunkImpl.PROTOCOL_VERSION + 1;
      ChunkImpl.read(ib);
      fail("Should have raised an IOexception");
    } catch (IOException e) {
      // right behavior, do nothing
    }
  }
  
  public void testTag() {
    ChunkBuilder cb = new ChunkBuilder();
    cb.addRecord("foo".getBytes());
    cb.addRecord("bar".getBytes());
    cb.addRecord("baz".getBytes());
    Chunk c = cb.getChunk();
    assertNull(c.getTag("foo"));
    c.addTag("foo=\"bar\"");
    assertEquals("bar", c.getTag("foo"));
  }
}
