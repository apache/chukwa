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

package org.apache.hadoop.chukwa.extraction.database;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.database.DatabaseConfig;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.RecordUtil;
import org.apache.hadoop.chukwa.util.ClusterConfig;
import org.apache.hadoop.chukwa.util.DatabaseWriter;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

public class MetricDataLoader {
  private static Log log = LogFactory.getLog(MetricDataLoader.class);

  private Statement stmt = null;
  private ResultSet rs = null;
  private DatabaseConfig mdlConfig = null;
  private HashMap<String, String> normalize = null;
  private HashMap<String, String> transformer = null;
  private HashMap<String, Float> conversion = null;
  private HashMap<String, String> dbTables = null;
  private HashMap<String, HashMap<String, Integer>> dbSchema = null;
  private String newSpace = "-";
  private boolean batchMode = true;
  private Connection conn = null;

  private static ChukwaConfiguration conf = null;
  private static FileSystem fs = null;
  private String jdbc_url = "";

  static {
    conf = new ChukwaConfiguration();
    String fsName = conf.get("writer.hdfs.filesystem");
    try {
      fs = FileSystem.get(new URI(fsName), conf);
    } catch (Exception e) {
      e.printStackTrace();
      log.warn("Exception during HDFS init, Bailing out!", e);
      System.exit(-1);
    }
  }

  /** Creates a new instance of DBWriter */
  public MetricDataLoader() {
    initEnv("");
  }

  public MetricDataLoader(String cluster) {
    initEnv(cluster);
  }

  private void initEnv(String cluster) {
    mdlConfig = new DatabaseConfig();
    transformer = mdlConfig.startWith("metric.");
    conversion = new HashMap<String, Float>();
    normalize = mdlConfig.startWith("normalize.");
    dbTables = mdlConfig.startWith("report.db.name.");
    Iterator<?> entries = mdlConfig.iterator();
    while (entries.hasNext()) {
      String entry = entries.next().toString();
      if (entry.startsWith("conversion.")) {
        String[] metrics = entry.split("=");
        try {
          float convertNumber = Float.parseFloat(metrics[1]);
          conversion.put(metrics[0], convertNumber);
        } catch (NumberFormatException ex) {
          log.error(metrics[0] + " is not a number.");
        }
      }
    }
    log.debug("cluster name:" + cluster);
    if (!cluster.equals("")) {
      ClusterConfig cc = new ClusterConfig();
      jdbc_url = cc.getURL(cluster);
    }
    HashMap<String, String> dbNames = mdlConfig.startWith("report.db.name.");
    Iterator<String> ki = dbNames.keySet().iterator();
    dbSchema = new HashMap<String, HashMap<String, Integer>>();
    DatabaseWriter dbWriter = new DatabaseWriter(cluster);
    while (ki.hasNext()) {
      String table = dbNames.get(ki.next().toString());
      String query = "select * from " + table + "_template limit 1";
      try {
        ResultSet rs = dbWriter.query(query);
        ResultSetMetaData rmeta = rs.getMetaData();
        HashMap<String, Integer> tableSchema = new HashMap<String, Integer>();
        for (int i = 1; i <= rmeta.getColumnCount(); i++) {
          tableSchema.put(rmeta.getColumnName(i), rmeta.getColumnType(i));
        }
        dbSchema.put(table, tableSchema);
      } catch (SQLException ex) {
        log.debug("table: " + table
          + " template does not exist, MDL will not load data for this table.");
      }
    }
    dbWriter.close();
  }

  public void interrupt() {
  }

  private String escape(String s, String c) {

    String ns = s.trim();
    Pattern pattern = Pattern.compile(" +");
    Matcher matcher = pattern.matcher(ns);
    String s2 = matcher.replaceAll(c);

    return s2;

  }

  public static String escapeQuotes( String s ) {
    StringBuffer sb = new StringBuffer(); 
    int index; 
    int length = s.length(); 
    char ch;
    for( index = 0; index < length; ++index ) {
      if(( ch = s.charAt( index )) == '\"' ) {
        sb.append( "\\\"" ); 
      } else if( ch == '\\' ) {
        sb.append( "\\\\" ); 
      } else {
        sb.append( ch );
      }
    }
    return( sb.toString()); 
  } 

  public void process(Path source) throws IOException, URISyntaxException,
      SQLException {

    log.info("StreamName: " + source.getName());

    SequenceFile.Reader r = new SequenceFile.Reader(fs, source, conf);

    try {
      // The newInstance() call is a work around for some
      // broken Java implementations
      org.apache.hadoop.chukwa.util.DriverManagerUtil.loadDriver().newInstance();
      log.debug("Initialized JDBC URL: " + jdbc_url);
    } catch (Exception ex) {
      // handle the error
      log.error(ex, ex);
    }
    conn = org.apache.hadoop.chukwa.util.DriverManagerUtil.getConnection(jdbc_url);
    stmt = conn.createStatement();
    conn.setAutoCommit(false);
    long currentTimeMillis = System.currentTimeMillis();
    boolean isSuccessful = true;
    String recordType = null;

    ChukwaRecordKey key = new ChukwaRecordKey();
    ChukwaRecord record = new ChukwaRecord();
    try {
      int batch = 0;
      while (r.next(key, record)) {
        String sqlTime = DatabaseWriter.formatTimeStamp(record.getTime());
        log.debug("Timestamp: " + record.getTime());
        log.debug("DataType: " + key.getReduceType());

        String[] fields = record.getFields();
        String table = null;
        String[] priKeys = null;
        HashMap<String, HashMap<String, String>> hashReport = new HashMap<String, HashMap<String, String>>();
        StringBuilder normKey = new StringBuilder();
        String node = record.getValue("csource");
        recordType = key.getReduceType().toLowerCase();
        String dbKey = "report.db.name." + recordType;
        if (dbTables.containsKey(dbKey)) {
          String[] tmp = mdlConfig.findTableName(mdlConfig.get(dbKey), record
              .getTime(), record.getTime());
          table = tmp[0];
        } else {
          log.debug(dbKey + " does not exist.");
          continue;
        }
        log.debug("table name:" + table);
        try {
          priKeys = mdlConfig.get("report.db.primary.key." + recordType).split(
              ",");
        } catch (Exception nullException) {
        }
        for (String field : fields) {
          String keyName = escape(field.toLowerCase(), newSpace);
          String keyValue = escape(record.getValue(field).toLowerCase(),
              newSpace);
          StringBuilder buildKey = new StringBuilder();
          buildKey.append("normalize.");
          buildKey.append(recordType);
          buildKey.append(".");
          buildKey.append(keyName);
          if (normalize.containsKey(buildKey.toString())) {
            if (normKey.toString().equals("")) {
              normKey.append(keyName);
              normKey.append(".");
              normKey.append(keyValue);
            } else {
              normKey.append(".");
              normKey.append(keyName);
              normKey.append(".");
              normKey.append(keyValue);
            }
          }
          StringBuilder normalizedKey = new StringBuilder();
          normalizedKey.append("metric.");
          normalizedKey.append(recordType);
          normalizedKey.append(".");
          normalizedKey.append(normKey);
          if (hashReport.containsKey(node)) {
            HashMap<String, String> tmpHash = hashReport.get(node);
            tmpHash.put(normalizedKey.toString(), keyValue);
            hashReport.put(node, tmpHash);
          } else {
            HashMap<String, String> tmpHash = new HashMap<String, String>();
            tmpHash.put(normalizedKey.toString(), keyValue);
            hashReport.put(node, tmpHash);
          }
        }
        for (String field : fields) {
          String valueName = escape(field.toLowerCase(), newSpace);
          String valueValue = escape(record.getValue(field).toLowerCase(),
              newSpace);
          StringBuilder buildKey = new StringBuilder();
          buildKey.append("metric.");
          buildKey.append(recordType);
          buildKey.append(".");
          buildKey.append(valueName);
          if (!normKey.toString().equals("")) {
            buildKey = new StringBuilder();
            buildKey.append("metric.");
            buildKey.append(recordType);
            buildKey.append(".");
            buildKey.append(normKey);
            buildKey.append(".");
            buildKey.append(valueName);
          }
          String normalizedKey = buildKey.toString();
          if (hashReport.containsKey(node)) {
            HashMap<String, String> tmpHash = hashReport.get(node);
            tmpHash.put(normalizedKey, valueValue);
            hashReport.put(node, tmpHash);
          } else {
            HashMap<String, String> tmpHash = new HashMap<String, String>();
            tmpHash.put(normalizedKey, valueValue);
            hashReport.put(node, tmpHash);

          }

        }
        Iterator<String> i = hashReport.keySet().iterator();
        while (i.hasNext()) {
          Object iteratorNode = i.next();
          HashMap<String, String> recordSet = hashReport.get(iteratorNode);
          Iterator<String> fi = recordSet.keySet().iterator();
          // Map any primary key that was not included in the report keyName
          StringBuilder sqlPriKeys = new StringBuilder();
          try {
            for (String priKey : priKeys) {
              if (priKey.equals("timestamp")) {
                sqlPriKeys.append(priKey);
                sqlPriKeys.append(" = \"");
                sqlPriKeys.append(sqlTime);
                sqlPriKeys.append("\"");
              }
              if (!priKey.equals(priKeys[priKeys.length - 1])) {
                sqlPriKeys.append(sqlPriKeys);
                sqlPriKeys.append(", ");
              }
            }
          } catch (Exception nullException) {
            // ignore if primary key is empty
          }
          // Map the hash objects to database table columns
          StringBuilder sqlValues = new StringBuilder();
          boolean firstValue = true;
          while (fi.hasNext()) {
            String fieldKey = (String) fi.next();
            if (transformer.containsKey(fieldKey)) {
              if (!firstValue) {
                sqlValues.append(", ");
              }
              try {
                if (dbSchema.get(dbTables.get(dbKey)).get(
                    transformer.get(fieldKey)) == java.sql.Types.VARCHAR
                    || dbSchema.get(dbTables.get(dbKey)).get(
                        transformer.get(fieldKey)) == java.sql.Types.BLOB) {
                  String conversionKey = "conversion." + fieldKey;
                  if (conversion.containsKey(conversionKey)) {
                    sqlValues.append(transformer.get(fieldKey));
                    sqlValues.append("=");
                    sqlValues.append(recordSet.get(fieldKey));
                    sqlValues.append(conversion.get(conversionKey).toString());
                  } else {
                    sqlValues.append(transformer.get(fieldKey));
                    sqlValues.append("=\'");
                    sqlValues.append(escapeQuotes(recordSet.get(fieldKey)));
                    sqlValues.append("\'");
                  }
                } else if (dbSchema.get(dbTables.get(dbKey)).get(
                    transformer.get(fieldKey)) == java.sql.Types.TIMESTAMP) {
                  SimpleDateFormat formatter = new SimpleDateFormat(
                      "yyyy-MM-dd HH:mm:ss");
                  Date recordDate = new Date();
                  recordDate.setTime((long) Long.parseLong(recordSet
                      .get(fieldKey)));
                  sqlValues.append(transformer.get(fieldKey));
                  sqlValues.append("=\"");
                  sqlValues.append(formatter.format(recordDate));
                  sqlValues.append("\"");
                } else if (dbSchema.get(dbTables.get(dbKey)).get(
                    transformer.get(fieldKey)) == java.sql.Types.BIGINT
                    || dbSchema.get(dbTables.get(dbKey)).get(
                        transformer.get(fieldKey)) == java.sql.Types.TINYINT
                    || dbSchema.get(dbTables.get(dbKey)).get(
                        transformer.get(fieldKey)) == java.sql.Types.INTEGER) {
                  long tmp = 0;
                  try {
                    tmp = Long.parseLong(recordSet.get(fieldKey).toString());
                    String conversionKey = "conversion." + fieldKey;
                    if (conversion.containsKey(conversionKey)) {
                      tmp = tmp
                          * Long.parseLong(conversion.get(conversionKey)
                              .toString());
                    }
                  } catch (Exception e) {
                    tmp = 0;
                  }
                  sqlValues.append(transformer.get(fieldKey));
                  sqlValues.append("=");
                  sqlValues.append(tmp);
                } else {
                  double tmp = 0;
                  tmp = Double.parseDouble(recordSet.get(fieldKey).toString());
                  String conversionKey = "conversion." + fieldKey;
                  if (conversion.containsKey(conversionKey)) {
                    tmp = tmp
                        * Double.parseDouble(conversion.get(conversionKey)
                            .toString());
                  }
                  if (Double.isNaN(tmp)) {
                    tmp = 0;
                  }
                  sqlValues.append(transformer.get(fieldKey));
                  sqlValues.append("=");
                  sqlValues.append(tmp);
                }
                firstValue = false;
              } catch (NumberFormatException ex) {
                String conversionKey = "conversion." + fieldKey;
                if (conversion.containsKey(conversionKey)) {
                  sqlValues.append(transformer.get(fieldKey));
                  sqlValues.append("=");
                  sqlValues.append(recordSet.get(fieldKey));
                  sqlValues.append(conversion.get(conversionKey).toString());
                } else {
                  sqlValues.append(transformer.get(fieldKey));
                  sqlValues.append("='");
                  sqlValues.append(escapeQuotes(recordSet.get(fieldKey)));
                  sqlValues.append("'");
                }
                firstValue = false;
              } catch (NullPointerException ex) {
                log.error("dbKey:" + dbKey + " fieldKey:" + fieldKey
                    + " does not contain valid MDL structure.");
              }
            }
          }

          StringBuilder sql = new StringBuilder();
          if (sqlPriKeys.length() > 0) {
            sql.append("INSERT INTO ");
            sql.append(table);
            sql.append(" SET ");
            sql.append(sqlPriKeys.toString());
            sql.append(",");
            sql.append(sqlValues.toString());
            sql.append(" ON DUPLICATE KEY UPDATE ");
            sql.append(sqlPriKeys.toString());
            sql.append(",");
            sql.append(sqlValues.toString());
            sql.append(";");
          } else {
            sql.append("INSERT INTO ");
            sql.append(table);
            sql.append(" SET ");
            sql.append(sqlValues.toString());
            sql.append(" ON DUPLICATE KEY UPDATE ");
            sql.append(sqlValues.toString());
            sql.append(";");
          }
          log.trace(sql);
          if (batchMode) {
            stmt.addBatch(sql.toString());
            batch++;
          } else {
            stmt.execute(sql.toString());
          }
          if (batchMode && batch > 20000) {
            int[] updateCounts = stmt.executeBatch();
            batch = 0;
          }
        }

      }
      if (batchMode) {
        int[] updateCounts = stmt.executeBatch();
      }
    } catch (SQLException ex) {
      // handle any errors
      isSuccessful = false;
      log.error(ex, ex);
      log.error("SQLException: " + ex.getMessage());
      log.error("SQLState: " + ex.getSQLState());
      log.error("VendorError: " + ex.getErrorCode());
    } catch (Exception e) {
      isSuccessful = false;
      log.error(ExceptionUtil.getStackTrace(e));
      e.printStackTrace();
    } finally {
      if (batchMode) {
        try {
          conn.commit();
        } catch (SQLException ex) {
          log.error(ex, ex);
          log.error("SQLException: " + ex.getMessage());
          log.error("SQLState: " + ex.getSQLState());
          log.error("VendorError: " + ex.getErrorCode());
        }
      }
      long latencyMillis = System.currentTimeMillis() - currentTimeMillis;
      int latencySeconds = ((int) (latencyMillis + 500)) / 1000;
      String logMsg = (isSuccessful ? "Saved" : "Error occurred in saving");
      log.info(logMsg + " (" + recordType + ","
          + RecordUtil.getClusterName(record) + ") " + latencySeconds + " sec");
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException ex) {
          log.error(ex, ex);
          log.error("SQLException: " + ex.getMessage());
          log.error("SQLState: " + ex.getSQLState());
          log.error("VendorError: " + ex.getErrorCode());
        }
        rs = null;
      }
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {
          log.error(ex, ex);
          log.error("SQLException: " + ex.getMessage());
          log.error("SQLState: " + ex.getSQLState());
          log.error("VendorError: " + ex.getErrorCode());
        }
        stmt = null;
      }
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException ex) {
          log.error(ex, ex);
          log.error("SQLException: " + ex.getMessage());
          log.error("SQLState: " + ex.getSQLState());
          log.error("VendorError: " + ex.getErrorCode());
        }
        conn = null;
      }
    }
  }

  public static void main(String[] args) {
    try {
      MetricDataLoader mdl = new MetricDataLoader(args[0]);
      mdl.process(new Path(args[1]));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
