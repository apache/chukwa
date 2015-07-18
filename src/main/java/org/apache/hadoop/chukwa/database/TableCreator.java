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


import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.util.DatabaseWriter;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.chukwa.util.RegexUtil;

public class TableCreator {
  private static DatabaseConfig dbc = null;
  private static Log log = LogFactory.getLog(TableCreator.class);

  public TableCreator() {
    if (dbc == null) {
      dbc = new DatabaseConfig();
    }
  }

  public void createTables() throws Exception {
    long now = (new Date()).getTime();
    createTables(now, now);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value =
      "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE", 
      justification = "Dynamic based upon tables in the database")
  public void createTables(long start, long end) throws Exception {
    String cluster = System.getProperty("CLUSTER");
    if (cluster == null) {
      cluster = "unknown";
    }
    DatabaseWriter dbw = new DatabaseWriter(cluster);
    HashMap<String, String> dbNames = dbc.startWith("report.db.name.");
    for(Entry<String, String> entry : dbNames.entrySet()) {
      String tableName = entry.getValue();
      if (!RegexUtil.isRegex(tableName)) {
        log.warn("Skipping tableName: '" + tableName
            + "' because there was an error parsing it as a regex: "
            + RegexUtil.regexError(tableName));
        return;
      }
      String[] tableList = dbc.findTableName(tableName, start, end);
      log.debug("table name: " + tableList[0]);
      try {
        String[] parts = tableList[0].split("_");
        int partition = Integer.parseInt(parts[parts.length - 2]);
        StringBuilder tableNameBuffer = new StringBuilder();
        for (int i = 0; i < parts.length - 2; i++) {
          if (i != 0) {
            tableNameBuffer.append("_");
          }
          tableNameBuffer.append(parts[i]);
        }
        String table = tableNameBuffer.toString();
        StringBuilder q = new StringBuilder();
        q.append("show create table ");
        q.append(table);
        q.append("_template;");
        final String query = q.toString();
        ResultSet rs = dbw.query(query);
        while (rs.next()) {
          log.debug("table schema: " + rs.getString(2));
          String tbl = rs.getString(2);
          log.debug("template table name:" + table + "_template");
          log.debug("replacing with table name:" + table + "_" + partition
              + "_" + parts[parts.length - 1]);
          log.debug("creating table: " + tbl);
          
          for(int i=0;i<2;i++) {
            StringBuilder templateName = new StringBuilder();
            templateName.append(table);
            templateName.append("_template");
            StringBuilder partitionName = new StringBuilder();
            partitionName.append(table);
            partitionName.append("_");
            partitionName.append(partition);
            partitionName.append("_");
            partitionName.append(parts[parts.length - 1]);
            tbl = tbl.replaceFirst("TABLE", "TABLE IF NOT EXISTS");
            tbl = tbl.replaceFirst(templateName.toString(), partitionName.toString());
            final String createTable = tbl;
            dbw.execute(createTable);
            partition++;
          }
        }
      } catch (NumberFormatException e) {
        log.error("Error in parsing table partition number, skipping table:"
            + tableList[0]);
      } catch (ArrayIndexOutOfBoundsException e) {
        log.debug("Skipping table:" + tableList[0]
            + ", because it has no partition configuration.");
      } catch (SQLException e) {
        throw e;
      }
    }
  }

  public static void usage() {
    System.out.println("TableCreator usage:");
    System.out
        .println("java -jar chukwa-core.jar org.apache.hadoop.chukwa.TableCreator <date> <time window size>");
    System.out.println("     date format: YYYY-MM-DD");
    System.out.println("     time window size: 7, 30, 91, 365, 3650");
  }

  public static void main(String[] args) {
    TableCreator tc = new TableCreator();
    if (args.length == 2) {
      try {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        long start = sdf.parse(args[0]).getTime();
        long end = start + (Long.parseLong(args[1]) * 1440 * 60 * 1000L);
        tc.createTables(start, end);
      } catch (Exception e) {
        System.out.println("Invalid date format or time window size.");
        e.printStackTrace();
        usage();
      }
    } else {
      try {
        tc.createTables();
      } catch (Exception e) {
        log.error(ExceptionUtil.getStackTrace(e));
      }
    }

  }
}
