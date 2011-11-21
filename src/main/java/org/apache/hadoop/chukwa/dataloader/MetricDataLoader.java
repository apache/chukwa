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

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.Callable;

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

public class MetricDataLoader implements Callable {
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
  private Path source = null;

  private static ChukwaConfiguration conf = null;
  private static FileSystem fs = null;
  private String jdbc_url = "";

  /** Creates a new instance of DBWriter */
  public MetricDataLoader(ChukwaConfiguration conf, FileSystem fs, String fileName) {
    source = new Path(fileName);
    this.conf = conf;
    this.fs = fs;
  }

  private void initEnv(String cluster) throws Exception {
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
    try {
      DatabaseWriter dbWriter = new DatabaseWriter(cluster);
      conn = dbWriter.getConnection();
    } catch(Exception ex) {
      throw new Exception("JDBC URL does not exist for:"+jdbc_url);
    }
    log.debug("Initialized JDBC URL: " + jdbc_url);
    HashMap<String, String> dbNames = mdlConfig.startWith("report.db.name.");
    Iterator<String> ki = dbNames.keySet().iterator();
    dbSchema = new HashMap<String, HashMap<String, Integer>>();
    while (ki.hasNext()) {
      String recordType = ki.next().toString();
      String table = dbNames.get(recordType);
      try {
        ResultSet rs = conn.getMetaData().getColumns(null, null, table+"_template", null);
        HashMap<String, Integer> tableSchema = new HashMap<String, Integer>();
        while(rs.next()) {
          String name = rs.getString("COLUMN_NAME");
          int type = rs.getInt("DATA_TYPE");
          tableSchema.put(name, type);
          StringBuilder metricName = new StringBuilder();
          metricName.append("metric.");
          metricName.append(recordType.substring(15));
          metricName.append(".");
          metricName.append(name);
          String mdlKey = metricName.toString().toLowerCase();
          if(!transformer.containsKey(mdlKey)) {
            transformer.put(mdlKey, name);
          }          
        }
        rs.close();
        dbSchema.put(table, tableSchema);
      } catch (SQLException ex) {
        log.debug("table: " + table
          + " template does not exist, MDL will not load data for this table.");
      }
    }
    stmt = conn.createStatement();
    conn.setAutoCommit(false);
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
      } else if( ch == '\'' ) {
        sb.append( "\\'" );
      } else {
        sb.append( ch );
      }
    }
    return( sb.toString()); 
  }
  
  public boolean run() throws IOException {
    boolean first=true;
    log.info("StreamName: " + source.getName());
    SequenceFile.Reader reader = null;

    try {
      // The newInstance() call is a work around for some
      // broken Java implementations
      reader = new SequenceFile.Reader(fs, source, conf);
    } catch (Exception ex) {
      // handle the error
      log.error(ex, ex);
    }
    long currentTimeMillis = System.currentTimeMillis();
    boolean isSuccessful = true;
    String recordType = null;

    ChukwaRecordKey key = new ChukwaRecordKey();
    ChukwaRecord record = new ChukwaRecord();
    String cluster = null;
    int numOfRecords = 0;
    try {
      Pattern p = Pattern.compile("(.*)\\-(\\d+)$");
      int batch = 0;
      while (reader.next(key, record)) {
    	numOfRecords++;
        if(first) { 
          try {
            cluster = RecordUtil.getClusterName(record);
            initEnv(cluster);
            first=false;
          } catch(Exception ex) {
            log.error("Initialization failed for: "+cluster+".  Please check jdbc configuration.");
            return false;
          }
        }
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
        Matcher m = p.matcher(recordType);
        if (dbTables.containsKey(dbKey)) {
          String[] tmp = mdlConfig.findTableName(mdlConfig.get(dbKey), record
              .getTime(), record.getTime());
          table = tmp[0];
        } else if(m.matches()) {
          String timePartition = "_week";
          int timeSize = Integer.parseInt(m.group(2));
          if(timeSize == 5) {
            timePartition = "_month";
          } else if(timeSize == 30) {
            timePartition = "_quarter";
          } else if(timeSize == 180) {
            timePartition = "_year";
          } else if(timeSize == 720) {
            timePartition = "_decade";
          }
          int partition = (int) (record.getTime() / timeSize);
          StringBuilder tmpDbKey = new StringBuilder();
          tmpDbKey.append("report.db.name.");
          tmpDbKey.append(m.group(1));
          if(dbTables.containsKey(tmpDbKey.toString())) {
            StringBuilder tmpTable = new StringBuilder();
            tmpTable.append(dbTables.get(tmpDbKey.toString()));
            tmpTable.append("_");
            tmpTable.append(partition);
            tmpTable.append("_");
            tmpTable.append(timePartition);
            table = tmpTable.toString();
          } else {
            log.debug(tmpDbKey.toString() + " does not exist.");
            continue;            
          }
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
            String fieldKey = fi.next();
            if (transformer.containsKey(fieldKey) && transformer.get(fieldKey).intern()!="_delete".intern()) {
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
                  recordDate.setTime(Long.parseLong(recordSet
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
            if(sqlValues.length() > 0) {
              sql.append("INSERT INTO ");
              sql.append(table);
              sql.append(" SET ");
              sql.append(sqlValues.toString());
              sql.append(" ON DUPLICATE KEY UPDATE ");
              sql.append(sqlValues.toString());
              sql.append(";");
            }
          }
          if(sql.length() > 0) {
            log.trace(sql);
          
            if (batchMode) {
              stmt.addBatch(sql.toString());
              batch++;
            } else {
              stmt.execute(sql.toString());
            }
            if (batchMode && batch > 20000) {
              int[] updateCounts = stmt.executeBatch();
              log.info("Batch mode inserted=" + updateCounts.length + "records.");
              batch = 0;
            }
          }
        }

      }

      if (batchMode) {
        int[] updateCounts = stmt.executeBatch();
        log.info("Batch mode inserted=" + updateCounts.length + "records.");
      }
    } catch (SQLException ex) {
      // handle any errors
      isSuccessful = false;
      log.error(ex, ex);
      log.error("SQLException: " + ex.getMessage());
      log.error("SQLState: " + ex.getSQLState());
      log.error("VendorError: " + ex.getErrorCode());
      // throw an exception up the chain to give the PostProcessorManager a chance to retry
      throw new IOException (ex);
    } catch (Exception e) {
      isSuccessful = false;
      log.error(ExceptionUtil.getStackTrace(e));
      // throw an exception up the chain to give the PostProcessorManager a chance to retry
      throw new IOException (e);
    } finally {
      if (batchMode && conn!=null) {
        try {
          conn.commit();
          log.info("batchMode commit done");
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
          + cluster + ") " + latencySeconds + " sec. numOfRecords: " + numOfRecords);
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
      
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception e) {
          log.warn("Could not close SequenceFile.Reader:" ,e);
        }
        reader = null;
      }
    }
    return true;
  }

  public Boolean call() throws IOException {
    return run();  
  }
  

  public static void main(String[] args) {
    try {
      conf = new ChukwaConfiguration();
      fs = FileSystem.get(conf);
      MetricDataLoader mdl = new MetricDataLoader(conf, fs, args[0]);
      mdl.run();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
