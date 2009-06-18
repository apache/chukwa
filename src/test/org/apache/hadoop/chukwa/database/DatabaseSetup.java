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
package org.apache.hadoop.chukwa.database;

import java.io.*;
import java.sql.*;
import java.util.*;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.chukwa.util.DatabaseWriter;
import org.apache.hadoop.chukwa.database.TableCreator;

public class DatabaseSetup {
  public long[] timeWindow = {7, 30, 91, 365, 3650};
  public String[] tables = {"system_metrics","disk","cluster_system_metrics","cluster_disk","mr_job","mr_task","dfs_namenode","dfs_datanode","dfs_fsnamesystem","dfs_throughput","hadoop_jvm","hadoop_mapred","hdfs_usage"};
  public String cluster = "demo";
  public long current = Calendar.getInstance().getTimeInMillis();

  public void setUpDatabase() throws Exception {
    System.setProperty("CLUSTER","demo");
    DatabaseWriter db = new DatabaseWriter(cluster);
    String buffer = "";
    File aFile = new File(System.getenv("CHUKWA_CONF_DIR")
        + File.separator + "database_create_tables.sql");
    buffer = readFile(aFile);
    String tables[] = buffer.split(";");
    for(String table : tables) {
      if(table.length()>5) {
        db.execute(table);
      }
    }
    db.close();
    for(int i=0;i<timeWindow.length;i++) {
      TableCreator tc = new TableCreator();
      long start = current;
      long end = current + (timeWindow[i]*1440*60*1000);
      tc.createTables(start, end);
    }
  }

  public void tearDownDatabase() {
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
    
}

