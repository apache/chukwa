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

import java.util.Map.Entry;

import org.apache.hadoop.chukwa.inputtools.hdfsusage.HDFSUsageMetrics;
import org.apache.hadoop.chukwa.inputtools.hdfsusage.HDFSUsagePlugin;
import org.apache.hadoop.chukwa.inputtools.jplugin.ChukwaMetrics;
import org.apache.hadoop.chukwa.inputtools.jplugin.ChukwaMetricsList;
import org.apache.hadoop.chukwa.inputtools.jplugin.GenericChukwaMetricsList;

import junit.framework.TestCase;

public class HDFSUsagePluginTest extends TestCase {

  public void testGetMetrics() throws Throwable {
    HDFSUsagePlugin plugin = new HDFSUsagePlugin();
    plugin.init(new String[0]);
    ChukwaMetricsList<HDFSUsageMetrics> list = plugin.getMetrics();
    System.out.println(list.getTimestamp());
    for (ChukwaMetrics metrics : list.getMetricsList()) {
      HDFSUsageMetrics usage = (HDFSUsageMetrics) metrics;
      System.out.print(usage.getName());
      System.out.println("size: " + usage.getSize());
    }
    System.out.println();

    String xml = list.toXml();
    System.out.println(xml);

    GenericChukwaMetricsList gene = new GenericChukwaMetricsList(xml);
    System.out.println(list.getTimestamp());
    for (ChukwaMetrics metrics : gene.getMetricsList()) {
      System.out.print(metrics.getKey());
      for (Entry<String, String> entry : metrics.getAttributes().entrySet()) {
        System.out.println(entry.getKey() + ": " + entry.getValue());
      }
    }
    System.out.println();
  }

}
