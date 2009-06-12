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

package org.apache.hadoop.chukwa.inputtools.log4j;

import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;
import org.apache.hadoop.chukwa.datacollection.controller.ClientFinalizer;
import org.apache.hadoop.chukwa.util.RecordConstants;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

public class TaskLogAppender extends 
    org.apache.hadoop.mapred.TaskLogAppender {
  static Logger log = Logger.getLogger(TaskLogAppender.class);
  static final String adaptorType = ChukwaAgentController.CharFileTailUTF8NewLineEscaped;
  ChukwaAgentController chukwaClient;
  String recordType = null;
  static boolean chukwaClientIsNull = true;
  static final Object chukwaLock = new Object();
  private ClientFinalizer clientFinalizer = null;

  public String getRecordType() {
    if (recordType != null)
      return recordType;
    else
      return "unknown";
  }

  public void setRecordType(String recordType) {
    this.recordType = recordType;
  }

  public void subAppend(LoggingEvent event) {
      this.qw.write(RecordConstants.escapeAllButLastRecordSeparator("\n",this.layout.format(event)));
      // Make sure only one thread can do this
      // and use the boolean to avoid the first level locking
      if (chukwaClientIsNull) {
        synchronized (chukwaLock) {
          if (chukwaClient == null) {
            String log4jFileName = getFile();
            String recordType = getRecordType();
            long currentLength = 0L;
            chukwaClient = new ChukwaAgentController();
            chukwaClientIsNull = false;
            String adaptorID = chukwaClient.add(ChukwaAgentController.CharFileTailUTF8NewLineEscaped,
              recordType,currentLength + " " + log4jFileName, currentLength);

            // Setup a shutdownHook for the controller
            clientFinalizer = new ClientFinalizer(chukwaClient);
            Runtime.getRuntime().addShutdownHook(clientFinalizer);
            if (adaptorID != null) {
              log.debug("Added file tailing adaptor to chukwa agent for file "
                  + log4jFileName + ", adaptorId:" + adaptorID
                  + " using this recordType :" + recordType
                  + ", starting at offset:" + currentLength);
            } else {
              log.debug("Chukwa adaptor not added, addFile(" + log4jFileName
                  + ") returned " + adaptorID
                  + ", current offset:" + currentLength);
            }
          }
        }
      }
  }
}
