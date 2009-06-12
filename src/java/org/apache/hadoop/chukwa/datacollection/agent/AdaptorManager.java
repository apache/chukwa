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
package org.apache.hadoop.chukwa.datacollection.agent;

import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;

/**
 * The interface to the agent that is exposed to adaptors.
 *
 */
public interface AdaptorManager {
  

  Configuration getConfiguration();
  int adaptorCount();
  long stopAdaptor(String id, boolean gracefully);
  Adaptor getAdaptor(String id);
  String processAddCommand(String cmd);
  Map<String, String> getAdaptorList();

  static AdaptorManager NULL = new AdaptorManager() {

    @Override
    public int adaptorCount() {
      return 0;
    }

    @Override
    public Adaptor getAdaptor(String id) {
      return null;
    }

    @Override
    public Map<String, String> getAdaptorList() {
      return Collections.emptyMap();
    }

    @Override
    public Configuration getConfiguration() {
      return new Configuration();
    }

    @Override
    public String processAddCommand(String cmd) {
      return "";
    }

    @Override
    public long stopAdaptor(String id, boolean gracefully) {
      return 0;
    }
  };
  
}
