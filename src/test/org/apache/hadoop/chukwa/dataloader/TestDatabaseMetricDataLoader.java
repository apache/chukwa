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
package org.apache.hadoop.chukwa.dataloader;

import junit.framework.TestCase;
import java.util.Calendar;
import org.apache.hadoop.chukwa.database.Macro;
import org.apache.hadoop.chukwa.util.DatabaseWriter;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.chukwa.database.TableCreator;
import org.apache.hadoop.chukwa.dataloader.MetricDataLoader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

public class TestDatabaseMetricDataLoader extends TestCase {

  long[] timeWindow = {7, 30, 91, 365, 3650};
  String[] tables = {"system_metrics","disk","mr_job","mr_task"}; //,"dfs_namenode","dfs_datanode","dfs_fsnamesystem","dfs_throughput","hadoop_jvm","hadoop_mapred","hdfs_usage"};
  String cluster = "demo";
  long current = Calendar.getInstance().getTimeInMillis();

  public void setUp() {
    System.setProperty("CLUSTER","demo");
    DatabaseWriter db = new DatabaseWriter(cluster);
    String buffer = "";
    File aFile = new File(System.getenv("CHUKWA_CONF_DIR")
                 + File.separator + "database_create_tables.sql");
    buffer = readFile(aFile);
    String tables[] = buffer.split(";");
    for(String table : tables) {
      if(table.length()>5) {
        try {
          db.execute(table);
        } catch (Exception e) {
          fail("Fail to retrieve meta data from database table: "+table);
        }
      }
    }
    db.close();
    for(int i=0;i<timeWindow.length;i++) {
      TableCreator tc = new TableCreator();
      long start = current;
      long end = current + (timeWindow[i]*1440*60*1000);
      try {
        tc.createTables(start, end);
      } catch (Exception e) {
        fail("Fail to create database tables.");
      }
    }
  }

  public void tearDown() {
    DatabaseWriter db = null;
    try {
      db = new DatabaseWriter(cluster);
      ResultSet rs = db.query("show tables");
      ArrayList<String> list = new ArrayList<String>();
      while(rs.next()) {
        String table = rs.getString(1);
        list.add(table);
      }
      for(String table : list) {
        db.execute("drop table "+table);
      }
    } catch(Throwable ex) {
    } finally {
      if(db!=null) {
        db.close();
      }
    }
  }

  public String readFile(File aFile) {
    StringBuffer contents = new StringBuffer();
    try {
      BufferedReader input = new BufferedReader(new FileReader(aFile));
      try {
        String line = null; // not declared within while loop
        while ((line = input.readLine()) != null) {
          contents.append(line);
          contents.append(System.getProperty("line.separator"));
        }
      } finally {
        input.close();
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    }
    return contents.toString();
  }

  public void testMetricDataLoader() {
    boolean skip=false;
    String srcDir = System.getenv("CHUKWA_DATA_DIR") + File.separator + "samples";
    try {
      ChukwaConfiguration conf = new ChukwaConfiguration();
      FileSystem fs = FileSystem.get(conf);
      FileStatus[] sources = fs.listStatus(new Path(srcDir));
      for (FileStatus sequenceFile : sources) {
        MetricDataLoader mdl = new MetricDataLoader(conf, fs, sequenceFile.getPath().toUri().toString());
        mdl.call();
      }
      if(sources.length==0) {
        skip=true;
      }
    } catch (Throwable ex) {
      fail("SQL Exception: "+ExceptionUtil.getStackTrace(ex));
    }
    if(!skip) {
      DatabaseWriter db = new DatabaseWriter(cluster);
      for(int i=0;i<tables.length;i++) {
        String query = "select [avg("+tables[i]+")] from ["+tables[i]+"]";
        Macro mp = new Macro(current,query);
        query = mp.toString();
        try {
          ResultSet rs = db.query(query);
          ResultSetMetaData rsmd = rs.getMetaData();
          int numberOfColumns = rsmd.getColumnCount();
          while(rs.next()) {
            for(int j=1;j<=numberOfColumns;j++) {
              assertTrue("Table: "+tables[i]+", Column: "+rsmd.getColumnName(j)+", contains no data.",rs.getString(j)!=null);
            }
          }
        } catch(Throwable ex) {
          fail("MetricDataLoader failed: "+ExceptionUtil.getStackTrace(ex));
        }
      }
      db.close();
      assertTrue("MetricDataLoader executed successfully.",true);
    }
  }

}
