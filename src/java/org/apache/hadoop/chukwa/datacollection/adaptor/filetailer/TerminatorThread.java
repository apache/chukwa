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


import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.adaptor.FileAdaptor;
import org.apache.log4j.Logger;

public class TerminatorThread extends Thread {
  private static Logger log = Logger.getLogger(TerminatorThread.class);

  private FileTailingAdaptor adaptor = null;

  public TerminatorThread(FileTailingAdaptor adaptor) {
    this.adaptor = adaptor;
  }

  public void run() {

    long endTime = System.currentTimeMillis() + (10 * 60 * 1000); // now + 10
                                                                  // mins
    int count = 0;
    log.info("Terminator thread started." + adaptor.toWatch.getPath());
    try {
      while (adaptor.tailFile()) {
        if (log.isDebugEnabled()) {
          log.debug("Terminator thread:" + adaptor.toWatch.getPath()
              + " still working");
        }
        long now = System.currentTimeMillis();
        if (now > endTime) {
          log.warn("TerminatorThread should have been finished by now! count="
              + count);
          count++;
          endTime = System.currentTimeMillis() + (10 * 60 * 1000); // now + 10
                                                                   // mins
          if (count > 3) {
            log.warn("TerminatorThread should have been finished by now, stopping it now! count="
                    + count);
            break;
          }
        }
      }
    } catch (InterruptedException e) {
      log.info("InterruptedException on Terminator thread:"
          + adaptor.toWatch.getPath(), e);
    } catch (Throwable e) {
      log.warn("Exception on Terminator thread:" + adaptor.toWatch.getPath(),
              e);
    }

    log.info("Terminator thread finished." + adaptor.toWatch.getPath());
    try {
      adaptor.reader.close();
    } catch (Throwable ex) {
      // do nothing
    }
  }
}
