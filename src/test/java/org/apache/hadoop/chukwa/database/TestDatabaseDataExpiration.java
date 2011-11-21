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

import junit.framework.TestCase;
import java.util.Calendar;
import org.apache.hadoop.chukwa.database.Macro;
import org.apache.hadoop.chukwa.util.DatabaseWriter;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.chukwa.database.Aggregator;
import org.apache.hadoop.chukwa.database.TableCreator;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class TestDatabaseDataExpiration extends TestCase {

  long[] timeWindow = {7, 30, 91, 365, 3650};
  String[] tables = {"system_metrics","disk","cluster_system_metrics","cluster_disk","mr_job","mr_task","dfs_namenode","dfs_datanode","dfs_fsnamesystem","dfs_throughput","hadoop_jvm","hadoop_mapred","hdfs_usage"};
  String cluster = "demo";
  long current = Calendar.getInstance().getTimeInMillis();

  public void setUp() throws Exception {
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

  public void verifyTable(String table) {
    ChukwaConfiguration cc = new ChukwaConfiguration();
    String query = "select * from ["+table+"];";
    Macro mp = new Macro(current,query);
    query = mp.toString();
    try {
      DatabaseWriter db = new DatabaseWriter(cluster);
      ResultSet rs = db.query(query);
      while(rs.next()) {
        int i = 1;
        String value = rs.getString(i);
      }
      db.close();
    } catch(SQLException ex) {
      fail("SQL Exception: "+ExceptionUtil.getStackTrace(ex));
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

  public void testDataExpiration() {
    for(int i=0;i<timeWindow.length;i++) {
      long start = current + (365*1440*60*1000);
      long end = start + (timeWindow[i]*1440*60*1000);
      try {
        DataExpiration de = new DataExpiration();
        de.dropTables(start, end);
      } catch(Throwable ex) {
        fail("SQL Exception: "+ExceptionUtil.getStackTrace(ex));
      }
      assertTrue("DataExpiration executed.", true);
      DatabaseWriter db = null;
      try {
        db = new DatabaseWriter(cluster);
        String query = "select * from [system_metrics];";
        Macro mp = new Macro(current,query);
        query = mp.toString();
        ResultSet rs = db.query(query);
      } catch(SQLException ex) {
        assertTrue("Table is not suppose to exist.",true);
        db.close();
      }
    } 
  }

}
