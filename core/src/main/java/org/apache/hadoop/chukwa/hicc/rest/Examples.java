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
package org.apache.hadoop.chukwa.hicc.rest;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.chukwa.hicc.bean.Chart;
import org.apache.hadoop.chukwa.hicc.bean.Dashboard;
import org.apache.hadoop.chukwa.hicc.bean.Series;
import org.apache.hadoop.chukwa.hicc.bean.SeriesMetaData;
import org.apache.hadoop.chukwa.hicc.bean.Widget;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="MS_SHOULD_BE_FINAL")
public class Examples {
  // Chart examples
  public static Chart SYSTEM_LOAD_AVERAGE;
  public static Chart CPU_UTILIZATION;
  public static Chart MEMORY_UTILIZATION;
  public static Chart DISK_UTILIZATION;
  public static Chart NETWORK_UTILIZATION;
  public static Chart SWAP_UTILIZATION;
  public static Chart NAMENODE_MEMORY;
  public static Chart HDFS_USAGE;
  public static Chart RESOURCE_MANAGER_MEMORY;
  public static Chart NODE_MANAGER_HEALTH;
  public static Chart HDFS_HA;
  public static Chart HDFS_LOAD;
  public static Chart NAMENODE_RPC_LATENCY;
  public static Chart DATANODES;
  public static Chart HBASE_MASTER_MEMORY;

  // Widget examples
  public static Widget SYSTEM_LOAD_AVERAGE_WIDGET;
  public static Widget WELCOME_PAGE_WIDGET;
  public static Widget TRIAL_DOWNLOAD_WIDGET;
  public static Widget CLUSTER_RUNNING_WIDGET;
  public static Widget USER_WORKING_WIDGET;
  public static Widget APP_RUNNING_WIDGET;
  public static Widget TRIAL_ABANDON_RATE_WIDGET;
  public static Widget CLUSTERS_HEALTH_WIDGET;
  public static Widget TOP_ACTIVE_CLUSTERS_WIDGET;
  public static Widget TOP_APP_WIDGET;
  public static Widget APP_USAGE_WIDGET;
  public static Widget QUICK_LINKS_WIDGET;
  public static Widget LOG_SEARCH_WIDGET;
  public static Widget YARN_APP_WIDGET;
  public static Widget HDFS_WIDGET;
  public static Widget HBASE_TABLE_WIDGET;
  public static Widget TOP_USER_WIDGET;
  public static Widget HDFS_HA_STATE_WIDGET;
  public static Widget HDFS_LOAD_WIDGET;
  public static Widget HDFS_NAMENODE_LATENCY_WIDGET;
  public static Widget DATANODES_HEALTH_WIDGET;
  public static Widget NODE_MANAGERS_HEALTH_WIDGET;
  public static Widget HDFS_REMAINING_WIDGET;
  public static Widget NAMENODE_MEMORY_WIDGET;
  public static Widget RESOURCE_MANAGER_MEMORY_WIDGET;
  public static Widget HBASE_MASTER_MOMORY_WIDGET;
  public static Widget CPU_UTILIZATION_WIDGET;
  public static Widget MEMORY_UTILIZATION_WIDGET;
  public static Widget SWAP_UTILIZATION_WIDGET;
  public static Widget DISK_UTILIZATION_WIDGET;
  public static Widget NETWORK_UTILIZATION_WIDGET;
  public static Widget CPU_HEAPMAP_WIDGET;
  public static Widget HDFS_UI_WIDGET;
  public static Widget HBASE_MASTER_UI_WIDGET;
  public static List<Widget> WIDGET_LIST;
  
  public static Dashboard DEFAULT_DASHBOARD;
  public static Dashboard USER_DASHBOARD;
  public static Dashboard SYSTEM_DASHBOARD;
  

  // series examples
  public static Series CPU_METRICS;
  
  // SeriesMetaData examples
  public static List<SeriesMetaData> CPU_SERIES_METADATA;
  public static List<SeriesMetaData> HDFS_USAGE_SERIES_METADATA;

  static {
    try {
      final String hostname = InetAddress.getLocalHost().getHostName().toLowerCase();
      final String[] metrics = { "SystemMetrics.LoadAverage.1" };
      SYSTEM_LOAD_AVERAGE = Chart.createChart("1", "System Load Average", metrics, hostname, "");
      final String[] cpuMetrics = { "SystemMetrics.cpu.combined", "SystemMetrics.cpu.sys", "SystemMetrics.cpu.user" };
      CPU_UTILIZATION = Chart.createChart("2", "CPU Utilization", cpuMetrics, hostname, "percent");
      final String[] memMetrics = { "SystemMetrics.memory.FreePercent", "SystemMetrics.memory.UsedPercent"};
      MEMORY_UTILIZATION = Chart.createChart("3", "Memory Utilization", memMetrics, hostname, "percent");
      final String[] diskMetrics = { "SystemMetrics.disk.ReadBytes", "SystemMetrics.disk.WriteBytes" };
      DISK_UTILIZATION = Chart.createChart("4", "Disk Utilization", diskMetrics, hostname, "bytes-decimal");
      final String[] netMetrics = { "SystemMetrics.network.TxBytes", "SystemMetrics.network.RxBytes" };
      NETWORK_UTILIZATION = Chart.createChart("5", "Network Utilization", netMetrics, hostname, "bytes");
      final String[] swapMetrics = { "SystemMetrics.swap.Total", "SystemMetrics.swap.Used", "SystemMetrics.swap.Free" };
      SWAP_UTILIZATION = Chart.createChart("6", "Swap Utilization", swapMetrics, hostname, "bytes-decimal");

      // Namenode heap usage
      StringBuilder namenode = new StringBuilder();
      namenode.append(hostname);
      namenode.append(":NameNode");
      final String[] namenodeHeap = { "HadoopMetrics.jvm.JvmMetrics.MemHeapUsedM", "HadoopMetrics.jvm.JvmMetrics.MemHeapMaxM" };
      NAMENODE_MEMORY = Chart.createCircle("7", "Namenode Memory", namenodeHeap, namenode.toString(), "%", "up");
      
      // HDFS Usage
      final String[] hdfsUsage = { "HadoopMetrics.dfs.FSNamesystem.CapacityRemainingGB", "HadoopMetrics.dfs.FSNamesystem.CapacityTotalGB" };
      HDFS_USAGE = Chart.createCircle("8", "HDFS Remaining", hdfsUsage, hostname, "%", "down");

      // Resource Manager Memory
      StringBuilder rmnode = new StringBuilder();
      rmnode.append(hostname);
      rmnode.append(":ResourceManager");
      final String[] rmHeap = { "HadoopMetrics.jvm.JvmMetrics.MemHeapUsedM", "HadoopMetrics.jvm.JvmMetrics.MemHeapMaxM" };
      RESOURCE_MANAGER_MEMORY = Chart.createCircle("9", "Resource Manager Memory", rmHeap, rmnode.toString(), "%", "up");

      // Node Managers Health
      final String[] nmh = { "HadoopMetrics.yarn.ClusterMetrics.NumActiveNMs", "HadoopMetrics.yarn.ClusterMetrics.NumLostNMs" };
      NODE_MANAGER_HEALTH = Chart.createTile("10", "Node Managers Health", "Node Managers", "Active/Lost", nmh, hostname, "glyphicon-th");

      // High Availability State
      final String[] ha = { "HadoopMetrics.dfs.FSNamesystem.HAState" };
      HDFS_HA = Chart.createTile("11", "HDFS High Availability State", "HDFS High Availability", "", ha, hostname, "glyphicon-random");

      // HDFS Load
      final String[] hdfsLoad = { "HadoopMetrics.dfs.FSNamesystem.TotalLoad" };
      HDFS_LOAD = Chart.createTile("12", "HDFS Load Average", "HDFS Load", "", hdfsLoad, hostname, "glyphicon-signal");

      // Namenode RPC Latency
      final String[] nnLatency = { "HadoopMetrics.rpc.rpc.RpcProcessingTimeAvgTime" };
      NAMENODE_RPC_LATENCY = Chart.createTile("13", "NameNode Latency", "NameNode RPC Latency", "Milliseconds", nnLatency, hostname, "glyphicon-tasks");

      // Datanode Health
      final String[] dnHealth = { "HadoopMetrics.dfs.FSNamesystem.StaleDataNodes" };
      DATANODES = Chart.createTile("14", "Datanodes Health", "Datanodes", "Dead", dnHealth, hostname, "glyphicon-hdd");

      // HBase Master Memory
      StringBuilder hbaseMaster = new StringBuilder();
      hbaseMaster.append(hostname);
      hbaseMaster.append(":Master");
      final String[] hbm = { "HBaseMetrics.jvm.JvmMetrics.MemHeapUsedM", "HBaseMetrics.jvm.JvmMetrics.MemHeapMaxM" };
      HBASE_MASTER_MEMORY = Chart.createCircle("15", "HBase Master Memory", hbm, hbaseMaster.toString(), "%", "up");

      CPU_SERIES_METADATA = CPU_UTILIZATION.getSeries();
      HDFS_USAGE_SERIES_METADATA = HDFS_USAGE.getSeries();
      
//      CPU_METRICS = new Series("SystemMetrics.LoadAverage.1");
//      CPU_METRICS.add(1234567890L, 0.0d);
//      CPU_METRICS.add(1234567891L, 1.0d);
//      CPU_METRICS.add(1234567892L, 2.0d);
//      CPU_METRICS.add(1234567893L, 3.0d);

      // Populate default widgets
      Widget widget = new Widget();
      widget.setTitle("System Load Average");
      widget.setSrc(new URI("/hicc/v1/chart/draw/1"));
      widget.setCol(1);
      widget.setRow(1);
      widget.setSize_x(2);
      widget.setSize_y(1);
      SYSTEM_LOAD_AVERAGE_WIDGET = widget;

      // Populate default dashboard
      Dashboard dashboard = new Dashboard();

      widget = new Widget();
      widget.setTitle("Welcome Page");
      widget.setSrc(new URI("/hicc/welcome.html"));
      widget.setCol(1);
      widget.setRow(1);
      widget.setSize_x(12);
      widget.setSize_y(7);
      WELCOME_PAGE_WIDGET = widget;
      dashboard.add(WELCOME_PAGE_WIDGET);
      DEFAULT_DASHBOARD = dashboard;

      widget = new Widget();
      widget.setTitle("Trial Downloading");
      widget.setSrc(new URI("/hicc/home/downloads.html"));
      widget.setCol(1);
      widget.setRow(1);
      widget.setSize_x(2);
      widget.setSize_y(1);
      TRIAL_DOWNLOAD_WIDGET = widget;

      widget = new Widget();
      widget.setTitle("Cluster Running");
      widget.setSrc(new URI("/hicc/home/clusters.html"));
      widget.setCol(3);
      widget.setRow(1);
      widget.setSize_x(2);
      widget.setSize_y(1);
      CLUSTER_RUNNING_WIDGET = widget;

      widget = new Widget();
      widget.setTitle("Users Working");
      widget.setSrc(new URI("/hicc/home/users.html"));
      widget.setCol(5);
      widget.setRow(1);
      widget.setSize_x(2);
      widget.setSize_y(1);
      USER_WORKING_WIDGET = widget;

      widget = new Widget();
      widget.setTitle("Applications Running");
      widget.setSrc(new URI("/hicc/home/apps.html"));
      widget.setCol(7);
      widget.setRow(1);
      widget.setSize_x(2);
      widget.setSize_y(1);
      APP_RUNNING_WIDGET = widget;

      widget = new Widget();
      widget.setTitle("Trial Abandon Rate");
      widget.setSrc(new URI("/hicc/v1/circles/draw/11"));
      widget.setCol(1);
      widget.setRow(2);
      widget.setSize_x(2);
      widget.setSize_y(2);
      TRIAL_ABANDON_RATE_WIDGET = widget;

      widget = new Widget();
      widget.setTitle("Clusters Health");
      widget.setSrc(new URI("/hicc/v1/circles/draw/12"));
      widget.setCol(3);
      widget.setRow(2);
      widget.setSize_x(2);
      widget.setSize_y(2);
      CLUSTERS_HEALTH_WIDGET = widget;

      widget = new Widget();
      widget.setTitle("Top Active Clusters");
      widget.setSrc(new URI("/hicc/clusters/"));
      widget.setCol(5);
      widget.setRow(2);
      widget.setSize_x(2);
      widget.setSize_y(2);
      TOP_ACTIVE_CLUSTERS_WIDGET = widget;

      widget = new Widget();
      widget.setTitle("Top Applications");
      widget.setSrc(new URI("/hicc/apps/"));
      widget.setCol(7);
      widget.setRow(2);
      widget.setSize_x(2);
      widget.setSize_y(2);
      TOP_APP_WIDGET = widget;

      widget = new Widget();
      widget.setTitle("Applications Usage");
      widget.setSrc(new URI("/hicc/apps/apps-usage.html"));
      widget.setCol(7);
      widget.setRow(2);
      widget.setSize_x(2);
      widget.setSize_y(2);
      APP_USAGE_WIDGET = widget;

      // Populate user dashboards
      dashboard = new Dashboard();

      widget = new Widget();
      widget.setTitle("Quick Links");
      widget.setSrc(new URI("/hicc/v1/dashboard/quicklinks"));
      widget.setCol(1);
      widget.setRow(1);
      widget.setSize_x(12);
      widget.setSize_y(7);
      QUICK_LINKS_WIDGET = widget;
      dashboard.add(QUICK_LINKS_WIDGET);

      // Log Search widget
      widget = new Widget();
      widget.setTitle("Log Search");
      widget.setSrc(new URI("/hicc/ajax-solr/chukwa"));
      widget.setCol(1);
      widget.setRow(1);
      widget.setSize_x(6);
      widget.setSize_y(6);
      LOG_SEARCH_WIDGET = widget;

      // Applications
      widget = new Widget();
      widget.setTitle("YARN Applications");
      widget.setSrc(new URI("http://localhost:8088/"));
      widget.setCol(1);
      widget.setRow(7);
      widget.setSize_x(6);
      widget.setSize_y(6);
      YARN_APP_WIDGET = widget;

      // Hadoop Distributed File System
      widget = new Widget();
      widget.setTitle("HDFS");
      widget.setSrc(new URI("http://localhost:50070/explorer.html#/"));
      widget.setCol(1);
      widget.setRow(7);
      widget.setSize_x(6);
      widget.setSize_y(6);
      HDFS_WIDGET = widget;

      // HBase Tables
      widget = new Widget();
      widget.setTitle("HBase Tables");
      widget.setSrc(new URI("http://localhost:50654/tablesDetailed.jsp"));
      widget.setCol(1);
      widget.setRow(14);
      widget.setSize_x(6);
      widget.setSize_y(6);
      HBASE_TABLE_WIDGET = widget;

      widget = new Widget();
      widget.setTitle("Top Applications");
      widget.setSrc(new URI("/hicc/apps/"));
      widget.setCol(1);
      widget.setRow(1);
      widget.setSize_x(2);
      widget.setSize_y(2);
      TOP_APP_WIDGET = widget;

      widget = new Widget();
      widget.setTitle("Top Users");
      widget.setSrc(new URI("/hicc/users/"));
      widget.setCol(1);
      widget.setRow(3);
      widget.setSize_x(2);
      widget.setSize_y(2);
      TOP_USER_WIDGET = widget;
      USER_DASHBOARD = dashboard;

      // Populate system dashboards
      dashboard = new Dashboard();
      widget = new Widget();
      widget.setTitle("HDFS High Availability State");
      widget.setSrc(new URI("/hicc/v1/tile/draw/11"));
      widget.setCol(1);
      widget.setRow(1);
      widget.setSize_x(2);
      widget.setSize_y(1);
      HDFS_HA_STATE_WIDGET = widget;
      dashboard.add(HDFS_HA_STATE_WIDGET);

      widget = new Widget();
      widget.setTitle("HDFS Load");
      widget.setSrc(new URI("/hicc/v1/tile/draw/12"));
      widget.setCol(3);
      widget.setRow(1);
      widget.setSize_x(2);
      widget.setSize_y(1);
      HDFS_LOAD_WIDGET = widget;
      dashboard.add(HDFS_LOAD_WIDGET);

      widget = new Widget();
      widget.setTitle("HDFS Namenode Latency");
      widget.setSrc(new URI("/hicc/v1/tile/draw/13"));
      widget.setCol(5);
      widget.setRow(1);
      widget.setSize_x(2);
      widget.setSize_y(1);
      HDFS_NAMENODE_LATENCY_WIDGET = widget;
      dashboard.add(HDFS_NAMENODE_LATENCY_WIDGET);

      widget = new Widget();
      widget.setTitle("Datanodes Health");
      widget.setSrc(new URI("/hicc/v1/tile/draw/14"));
      widget.setCol(7);
      widget.setRow(1);
      widget.setSize_x(2);
      widget.setSize_y(1);
      DATANODES_HEALTH_WIDGET = widget;
      dashboard.add(DATANODES_HEALTH_WIDGET);
      
      widget = new Widget();
      widget.setTitle("Node Managers Health");
      widget.setSrc(new URI("/hicc/v1/tile/draw/10"));
      widget.setCol(9);
      widget.setRow(1);
      widget.setSize_x(2);
      widget.setSize_y(1);
      NODE_MANAGERS_HEALTH_WIDGET = widget;
      dashboard.add(NODE_MANAGERS_HEALTH_WIDGET);

      widget = new Widget();
      widget.setTitle("HDFS Remaining");
      widget.setSrc(new URI("/hicc/v1/circles/draw/8"));
      widget.setCol(1);
      widget.setRow(2);
      widget.setSize_x(2);
      widget.setSize_y(2);
      HDFS_REMAINING_WIDGET = widget;
      dashboard.add(HDFS_REMAINING_WIDGET);

      widget = new Widget();
      widget.setTitle("Namenode Memory");
      widget.setSrc(new URI("/hicc/v1/circles/draw/7"));
      widget.setCol(3);
      widget.setRow(2);
      widget.setSize_x(2);
      widget.setSize_y(2);
      NAMENODE_MEMORY_WIDGET = widget;
      dashboard.add(NAMENODE_MEMORY_WIDGET);

      widget = new Widget();
      widget.setTitle("Resource Manager Memory");
      widget.setSrc(new URI("/hicc/v1/circles/draw/9"));
      widget.setCol(5);
      widget.setRow(2);
      widget.setSize_x(2);
      widget.setSize_y(2);
      RESOURCE_MANAGER_MEMORY_WIDGET = widget;
      dashboard.add(RESOURCE_MANAGER_MEMORY_WIDGET);

      widget = new Widget();
      widget.setTitle("HBase Master Memory");
      widget.setSrc(new URI("/hicc/v1/circles/draw/15"));
      widget.setCol(7);
      widget.setRow(2);
      widget.setSize_x(2);
      widget.setSize_y(2);
      HBASE_MASTER_MOMORY_WIDGET = widget;
      dashboard.add(HBASE_MASTER_MOMORY_WIDGET);

      widget = new Widget();
      widget.setTitle("System Load Average");
      widget.setSrc(new URI("/hicc/v1/chart/draw/1"));
      widget.setCol(9);
      widget.setRow(2);
      widget.setSize_x(2);
      widget.setSize_y(1);
      SYSTEM_LOAD_AVERAGE_WIDGET = widget;
      dashboard.add(SYSTEM_LOAD_AVERAGE_WIDGET);

      widget = new Widget();
      widget.setTitle("CPU Utilization");
      widget.setSrc(new URI("/hicc/v1/chart/draw/2"));
      widget.setCol(9);
      widget.setRow(3);
      widget.setSize_x(2);
      widget.setSize_y(1);
      CPU_UTILIZATION_WIDGET = widget;
      dashboard.add(CPU_UTILIZATION_WIDGET);

      widget = new Widget();
      widget.setTitle("Memory Utilization");
      widget.setSrc(new URI("/hicc/v1/chart/draw/3"));
      widget.setCol(9);
      widget.setRow(4);
      widget.setSize_x(2);
      widget.setSize_y(1);
      MEMORY_UTILIZATION_WIDGET = widget;
      dashboard.add(MEMORY_UTILIZATION_WIDGET);

      widget = new Widget();
      widget.setTitle("Swap Utilization");
      widget.setSrc(new URI("/hicc/v1/chart/draw/6"));
      widget.setCol(9);
      widget.setRow(5);
      widget.setSize_x(2);
      widget.setSize_y(1);
      SWAP_UTILIZATION_WIDGET = widget;
      dashboard.add(SWAP_UTILIZATION_WIDGET);

      widget = new Widget();
      widget.setTitle("Disk Utilization");
      widget.setSrc(new URI("/hicc/v1/chart/draw/4"));
      widget.setCol(1);
      widget.setRow(4);
      widget.setSize_x(4);
      widget.setSize_y(2);
      DISK_UTILIZATION_WIDGET = widget;
      dashboard.add(DISK_UTILIZATION_WIDGET);

      widget = new Widget();
      widget.setTitle("Network Utilization");
      widget.setSrc(new URI("/hicc/v1/chart/draw/5"));
      widget.setCol(5);
      widget.setRow(4);
      widget.setSize_x(4);
      widget.setSize_y(2);
      NETWORK_UTILIZATION_WIDGET = widget;
      dashboard.add(NETWORK_UTILIZATION_WIDGET);
      SYSTEM_DASHBOARD = dashboard;

      // CPU heatmap
      widget = new Widget();
      widget.setTitle("CPU Heatmap");
      widget.setSrc(new URI("/hicc/v1/heatmap/render/SystemMetrics/cpu.combined."));
      widget.setCol(1);
      widget.setRow(5);
      widget.setSize_x(6);
      widget.setSize_y(5);
      CPU_HEAPMAP_WIDGET = widget;

      // HDFS Namenode
      widget = new Widget();
      widget.setTitle("HDFS UI");
      widget.setSrc(new URI("http://localhost:50070/"));
      widget.setCol(1);
      widget.setRow(11);
      widget.setSize_x(6);
      widget.setSize_y(6);
      HDFS_UI_WIDGET = widget;

      // HBase Master
      widget = new Widget();
      widget.setTitle("HBase Master UI");
      widget.setSrc(new URI("http://localhost:16010/"));
      widget.setCol(1);
      widget.setRow(18);
      widget.setSize_x(6);
      widget.setSize_y(6);
      HBASE_MASTER_UI_WIDGET = widget;

      WIDGET_LIST = new ArrayList<Widget>();
      WIDGET_LIST.add(HDFS_HA_STATE_WIDGET);
      WIDGET_LIST.add(HDFS_UI_WIDGET);
      WIDGET_LIST.add(HDFS_LOAD_WIDGET);
    } catch (URISyntaxException e) {
    } catch (UnknownHostException e) {
    }
  }

}
