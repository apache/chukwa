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
package org.apache.hadoop.chukwa.datacollection.writer;


import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.hadoop.chukwa.datacollection.writer.ClientAck;

public class TestClientAck extends TestCase {

  public void testWait4AckTimeOut() {
    ClientAck clientAck = new ClientAck();
    long startDate = System.currentTimeMillis();
    clientAck.wait4Ack();
    long now = System.currentTimeMillis();
    long duration = now - startDate;
    duration = duration - clientAck.getTimeOut();

    Assert.assertTrue("should not wait nore than " + clientAck.getTimeOut()
        + " + 7sec", duration < 7000);
    Assert.assertEquals(ClientAck.KO_LOCK, clientAck.getStatus());
  }

}
