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

public enum AdaptorShutdownPolicy {
  HARD_STOP,GRACEFULLY,WAIT_TILL_FINISHED,RESTARTING;
  
  public String toString() {
    if(this.equals(GRACEFULLY))
      return "Gracefully";
    else if(this.equals(HARD_STOP))
      return "Abruptly";
    else if(this.equals(WAIT_TILL_FINISHED))
      return "Once finished";
    else if(this.equals(RESTARTING))
      return "Prepare to restart";
    else
        return "unknown mode";
  }
}
