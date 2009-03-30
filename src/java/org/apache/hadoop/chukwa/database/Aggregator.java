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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.DatabaseMetaData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.inputtools.mdl.DataConfig;
import org.apache.hadoop.chukwa.util.DatabaseWriter;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.chukwa.util.PidFile;

public class Aggregator {

  private static Log log = LogFactory.getLog(Aggregator.class);
  private String table = null;
  private String jdbc = null;
  private int[] intervals;
  private long current = 0;
  private static DatabaseWriter db = null;

  public Aggregator() {
    Calendar now = Calendar.getInstance();
    current = now.getTimeInMillis();
  }

  public static String getContents(File aFile) {
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

  public void process(String query) {
    ResultSet rs = null;
    String[] columns;
    int[] columnsType;
    String groupBy = "";
    long start = current;
    long end = current;

    try {
      Macro macroProcessor = new Macro(current, current, query);
      query = macroProcessor.toString();
      db.execute(query);
    } catch (Exception e) {
      log.error(query);
      log.error(e.getMessage());
    }
  }

  public static void main(String[] args) {
    long longest = 0;
    String longQuery = null;
    log.info("Aggregator started.");
    String cluster = System.getProperty("CLUSTER");
    if (cluster == null) {
      cluster = "unknown";
    }
    db = new DatabaseWriter(cluster);
    String queries = Aggregator.getContents(new File(System
        .getenv("CHUKWA_CONF_DIR")
        + File.separator + "aggregator.sql"));
    String[] query = queries.split("\n");
    for (int i = 0; i < query.length; i++) {
      if (query[i].equals("")) {
      } else if (query[i].indexOf("#") == 0) {
        log.debug("skipping: " + query[i]);
      } else {
        Aggregator dba = new Aggregator();
        long start = Calendar.getInstance().getTimeInMillis();
        dba.process(query[i]);
        long end = Calendar.getInstance().getTimeInMillis();
        long duration = end - start;
        if (duration >= longest) {
          longest = duration;
          longQuery = query[i];
        }
      }
    }
    db.close();
    log.info("Longest running query: " + longQuery + " (" + (double) longest
        / 1000 + " seconds)");
    log.info("Aggregator finished.");
  }

}
