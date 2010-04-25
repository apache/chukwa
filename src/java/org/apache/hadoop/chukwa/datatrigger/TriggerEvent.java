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
package org.apache.hadoop.chukwa.datatrigger;


/**
 * enum that encapsulates the different possible events that can be triggered.
 * When a call is made to a TriggerAction class, the caller must pass a TriggerEvent
 * object to identify the event that is occurring.
 */
public enum TriggerEvent {

  POST_DEMUX_SUCCESS("postDemuxSuccess", "chukwa.trigger.post.demux.success");

  private String name;
  private String configKeyBase;

  private TriggerEvent(String name, String configKeyBase) {
    this.name = name;
    this.configKeyBase = configKeyBase;
  }

  public String getName() {
    return name;
  }

  public String getConfigKeyBase() {
    return configKeyBase;
  }
}
