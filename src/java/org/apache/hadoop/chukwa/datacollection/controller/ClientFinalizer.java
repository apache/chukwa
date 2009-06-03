/**
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

package org.apache.hadoop.chukwa.datacollection.controller;

import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;
import org.apache.log4j.Logger;
import org.apache.log4j.helpers.LogLog;


public class ClientFinalizer extends Thread {
  private ChukwaAgentController chukwaClient = null;
  private String recordType = null;
  private String fileName = null;

  public ClientFinalizer(ChukwaAgentController chukwaClient) {
    this.chukwaClient = chukwaClient;
  }

  public synchronized void run() {
    try {
      if (chukwaClient != null) {
        chukwaClient.removeInstanceAdaptors();
      } else {
        LogLog.warn("chukwaClient is null cannot do any cleanup");
      }
    } catch (Throwable e) {
      LogLog.warn("closing the controller threw an exception:\n" + e);
    }
  }
}

