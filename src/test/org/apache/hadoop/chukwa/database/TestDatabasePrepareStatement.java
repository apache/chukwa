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
import org.apache.hadoop.chukwa.database.TableCreator;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;

public class TestDatabasePrepareStatement extends TestCase {

  long[] timeWindow = {7, 30, 91, 365, 3650};
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
          fail("Fail to retrieve meta data from table:"+table);
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

  public void testPrepareStatement() {
    DatabaseWriter db = new DatabaseWriter(cluster);
    Date today = new Date();
    long current = today.getTime();
    Timestamp timestamp = new Timestamp(current);
    String hostname="chukwa.example.org";
    String query = "insert into [system_metrics] set timestamp='"+timestamp.toString()+"', host='"+hostname+"', cpu_user_pcnt=100;";
    Macro mp = new Macro(current, current, query);
    query = mp.toString();
    try {
      db.execute(query);
      query = "select timestamp,host,cpu_user_pcnt from [system_metrics] where timestamp=? and host=? and cpu_user_pcnt=?;";
      mp = new Macro(current, current, query);
      query = mp.toString();
      ArrayList<Object> parms = new ArrayList<Object>();
      parms.add(current);
      parms.add(hostname);
      parms.add(100); 
      ResultSet rs = db.query(query, parms);
      while(rs.next()) {
        assertTrue(hostname.intern()==rs.getString(2).intern());
        assertTrue(100==rs.getInt(3));
      }
      db.close();
    } catch(SQLException ex) {
      fail("Fail to run SQL statement:"+ExceptionUtil.getStackTrace(ex));
    }
  }

}
