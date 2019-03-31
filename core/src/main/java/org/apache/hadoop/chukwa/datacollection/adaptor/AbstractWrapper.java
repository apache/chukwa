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

import java.util.regex.*;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorFactory;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorManager;

public class AbstractWrapper implements NotifyOnCommitAdaptor,ChunkReceiver {
 
  Adaptor inner;
  String innerClassName;
  String innerType;
  ChunkReceiver dest;
  AdaptorManager manager;
  String adaptorID;
  @Override
  public String getCurrentStatus() {
    return innerClassName + " " + inner.getCurrentStatus();
  }


  static Pattern p = Pattern.compile("([^ ]+) +([^ ].*)");
  
  /**
   * Note that the name of the inner class will get parsed out as a type
   */
  @Override
  public String parseArgs(String innerClassName, String params, AdaptorManager a) {
    manager = a;
    Matcher m = p.matcher(params);
    this.innerClassName = innerClassName;
    String innerCoreParams;
    if(m.matches()) {
      innerType = m.group(1);
      inner = AdaptorFactory.createAdaptor(innerClassName);
      innerCoreParams = inner.parseArgs(innerType,m.group(2),a);
      return innerClassName + innerCoreParams;
    }
    else return null;
  }
  
  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy)
      throws AdaptorException {
    return inner.shutdown(shutdownPolicy);
  }

  @Override
  public String getType() {
    return innerType;
  }

  /**
   * Note that the name of the inner class will get parsed out as a type
   */
  @Override
  public void start(String adaptorID, String type, long offset,
      ChunkReceiver dest) throws AdaptorException {
    String dummyAdaptorID = adaptorID;
    this.dest = dest;
    this.adaptorID = adaptorID;
    inner.start(dummyAdaptorID, type, offset, this);
  }

  @Override
  public void add(Chunk event) throws InterruptedException {
    dest.add(event);
  }

  @Override
  public void committed(long commitedByte) { }

}
