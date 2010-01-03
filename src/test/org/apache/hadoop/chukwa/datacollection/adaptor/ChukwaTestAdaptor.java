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

public class ChukwaTestAdaptor extends AbstractAdaptor {

  private String params = null;
  private long startOffset = 0l;

  @Override
  public String getCurrentStatus() {
    // TODO Auto-generated method stub
    return type + " " + params + " " + startOffset;
  }

  @Override
  public String parseArgs(String s) {
    params = s;
    return s;
  }

  @Override
  public void start(long offset) throws AdaptorException {
    this.startOffset = offset;
    System.out.println("adaptorId [" + adaptorID + "]");
    System.out.println("type [" + type + "]");
    System.out.println("params [" + params + "]");
    System.out.println("startOffset [" + startOffset + "]");

  }

  public String getParams() {
    return params;
  }

  public void setParams(String params) {
    this.params = params;
  }

  public long getStartOffset() {
    return startOffset;
  }


  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy)
      throws AdaptorException {
    return 0;
  }

}
