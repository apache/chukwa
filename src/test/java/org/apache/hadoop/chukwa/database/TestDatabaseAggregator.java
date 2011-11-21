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

public class TestDatabaseAggregator extends TestCase {
    public DatabaseSetup dbSetup = new DatabaseSetup();

    public void setUp() {
      try{
	dbSetup.setUpDatabase();
      } catch (Exception e) {
        fail(ExceptionUtil.getStackTrace(e));
      }
    }

    public void tearDown() {
	dbSetup.tearDownDatabase();
    }

  public void verifyTable(String table) {
    ChukwaConfiguration cc = new ChukwaConfiguration();
    String query = "select * from ["+table+"];";
    Macro mp = new Macro(dbSetup.current,query);
    query = mp.toString();
    try {
      DatabaseWriter db = new DatabaseWriter(dbSetup.cluster);
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

  public void testAggregator() {
    Aggregator dba = new Aggregator();
    DatabaseWriter db = new DatabaseWriter(dbSetup.cluster);
    dba.setWriter(db);
    String queries = Aggregator.getContents(new File(System
        .getenv("CHUKWA_CONF_DIR")
        + File.separator + "aggregator.sql"));
    String[] query = queries.split("\n");
    for (int i = 0; i < query.length; i++) {
      if(query[i].indexOf("#")==-1) {
        try {
          dba.process(query[i]);
          assertTrue("Completed query: "+query[i],true);
        } catch(Throwable ex) {
          fail("Exception: "+ExceptionUtil.getStackTrace(ex));
        }
      }
    }
    db.close();
  }

}
