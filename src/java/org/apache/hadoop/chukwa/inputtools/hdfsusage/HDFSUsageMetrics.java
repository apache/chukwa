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

package org.apache.hadoop.chukwa.inputtools.hdfsusage;

import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.chukwa.inputtools.jplugin.ChukwaMetrics;

public class HDFSUsageMetrics implements ChukwaMetrics {
  private String name = null;
  
  private Long size;
  private long lastModified;
  
  @Override
  public String getKey() {
    return getName();
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setSize(Long size) {
    this.size = size;
  }

  public Long getSize() {
    return size;
  }

  @Override
  public HashMap<String, String> getAttributes() {
    HashMap<String, String> attr = new HashMap<String, String>();
    attr.put("user", name);
    attr.put("bytes", size.toString());
    attr.put("timestamp", new Date().getTime() + "");
    return attr;
  }

  public void setLastModified(long lastModified) {
    this.lastModified = lastModified;
  }

  public long getLastModified() {
    return lastModified;
  }
}
