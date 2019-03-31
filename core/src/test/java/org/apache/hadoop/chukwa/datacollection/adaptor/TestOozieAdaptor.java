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

package org.apache.hadoop.chukwa.datacollection.adaptor;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorManager;
import org.json.simple.JSONObject;
import org.junit.Test;

import junit.framework.TestCase;

public class TestOozieAdaptor extends TestCase implements ChunkReceiver {

  volatile boolean receivedOK = false;
  public String str = null;

  @Test
  public void testMessageReceivedOk() throws Exception {
    OozieAdaptor oozieAdaptor = new OozieAdaptor();

    oozieAdaptor.parseArgs("TestOozieAdaptor", "0", AdaptorManager.NULL);
    oozieAdaptor.start("id", "TestOozieAdaptor", 0, this);

    JSONObject json = composeMessage();
    int lengthReturned = oozieAdaptor.addChunkToReceiver(json.toString()
        .getBytes());
    assertEquals(84, lengthReturned); // 84 is the length of json string

    synchronized (this) {
      wait(1000);
    }
    assertTrue(receivedOK);
  }

  @SuppressWarnings("unchecked")
  private JSONObject composeMessage() {
    JSONObject json = new JSONObject();
    json.put("oozie.jvm.used.memory", 10);
    json.put("oozie.jvm.free.memory", 90);
    json.put("oozie.jvm.total.memory", 100);
    str = json.toString();
    return json;
  }

  @Override
  public void add(Chunk C) throws InterruptedException {
    assertTrue(C.getDataType().equals("TestOozieAdaptor"));
    assertEquals(C.getSeqID(), C.getData().length);
    byte[] data = C.getData();
    String s = new String(data);

    assertTrue(str.equals(s));

    receivedOK = true;
    synchronized (this) {
      notify();
    }
  }
}
