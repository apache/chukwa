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

import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorManager;

public abstract class AbstractAdaptor implements Adaptor {
  

  protected String type;
  protected ChunkReceiver dest;
  protected String adaptorID;
  protected AdaptorManager control;

  @Override
  public final String getType() {
    return type;
  }

  @Override
  public final void start(String adaptorID, String type, long offset,
      ChunkReceiver dest, AdaptorManager c) throws AdaptorException {
    this.adaptorID = adaptorID;
    this.type = type;
    this.dest=dest;
    control = c;
    start(offset);
  }
  
  public abstract void start(long offset) throws AdaptorException;

  public void deregisterAndStop(boolean gracefully) {
    control.stopAdaptor(adaptorID, gracefully);
  }
}
