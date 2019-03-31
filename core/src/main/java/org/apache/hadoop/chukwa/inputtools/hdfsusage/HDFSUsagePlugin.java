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

import java.net.URI;

import org.apache.hadoop.chukwa.inputtools.jplugin.ChukwaMetricsList;
import org.apache.hadoop.chukwa.inputtools.jplugin.JPlugin;
import org.apache.hadoop.chukwa.inputtools.jplugin.JPluginStatusMetricsList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUsagePlugin implements JPlugin<HDFSUsageMetrics> {
  private FileSystem hdfs;
  private String path;
  private OrgChart chart;
  
  @Override
  public ChukwaMetricsList<HDFSUsageMetrics> getMetrics() throws Throwable {
    ChukwaMetricsList<HDFSUsageMetrics> metricsList = new ChukwaMetricsList<HDFSUsageMetrics>(getRecordType());
    FileStatus status[] = hdfs.globStatus(new Path(path));
    for(int i=0; i<status.length; i++) {
      long totalSize = hdfs.getContentSummary(status[i].getPath()).getLength();
      if(totalSize <= 0) {
        continue;
      }
      String name = status[i].getPath().getName();
      HDFSUsageMetrics usage = new HDFSUsageMetrics();
      usage.setName(name);
      usage.setSize(totalSize);
      usage.setLastModified(status[i].getModificationTime());
      metricsList.addMetrics(usage);
      
      // also contribute to manager's usage
      if(chart != null) {
        Employee employee = chart.get(name);
        if(employee != null) {
          employee = employee.getManager();
          while(employee != null) {
            HDFSUsageMetrics managerUsage = new HDFSUsageMetrics();
            managerUsage.setName(employee.getId());
            managerUsage.setSize(totalSize);
            metricsList.addMetrics(managerUsage);
            employee = employee.getManager();
          }
        }
      }
    }
    return metricsList;
  }

  @Override
  public void init(String[] args) throws Throwable {
    for(int i=0; i<args.length; i++) {
      if(args[i].equals("-c")) {
        String orgChartClass = args[i+1];
        chart = (OrgChart) Class.forName(orgChartClass).newInstance();
        i++;
      } else if(args[i].equals("-h")) {
        Configuration conf = new Configuration();
        hdfs = FileSystem.get(new URI(args[i+1]), conf);
        i++;
      } else if(args[i].equals("-p")) {
        path = args[i+1];
        i++;
      }
    }
    
    if(hdfs == null) {
      Configuration conf = new Configuration();
      hdfs = FileSystem.get(conf);
    }
    
    if(path == null) {
      path = "/user/*";
    }
  }

  @Override
  public JPluginStatusMetricsList getStatus() throws Throwable {
    JPluginStatusMetricsList list = new JPluginStatusMetricsList(this.getClass().getSimpleName());
    list.addStatus("hdfs", hdfs.getUri().toString());
    list.addStatus("path", path);
    return null;
  }

  @Override
  public String getRecordType() {
    return "HDFSUsage";
  }
}
