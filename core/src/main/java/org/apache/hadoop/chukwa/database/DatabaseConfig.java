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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.util.*;
import java.io.File;
import java.io.FilenameFilter;

public class DatabaseConfig {
  private Configuration config = null;
  public final static long CENTURY = 36500 * 24 * 60 * 60 * 1000L;
  public final static long DECADE = 3650 * 24 * 60 * 60 * 1000L;
  public final static long YEAR = 365 * 24 * 60 * 60 * 1000L;
  public final static long QUARTER = 91250 * 24 * 60 * 60L;
  public final static long MONTH = 30 * 24 * 60 * 60 * 1000L;
  public final static long WEEK = 7 * 24 * 60 * 60 * 1000L;
  public final static long DAY = 24 * 60 * 60 * 1000L;
  public final static String MDL_XML = "mdl.xml";

  public DatabaseConfig(String path) {
    Path fileResource = new Path(path);
    config = new Configuration();
    config.addResource(fileResource);
  }

  public DatabaseConfig() {
    String dataConfig = System.getenv("CHUKWA_CONF_DIR");
    if (dataConfig == null) {
      dataConfig = MDL_XML;
    } else {
      dataConfig += File.separator + MDL_XML;
    }
    Path fileResource = new Path(dataConfig);
    config = new Configuration();
    config.addResource(fileResource);
    
    if (System.getenv("CHUKWA_CONF_DIR") != null) {
      // Allow site-specific MDL files to be included in the 
      // configuration so as to keep the "main" mdl.xml pure.
      File confDir = new File(System.getenv("CHUKWA_CONF_DIR"));
      File[] confFiles = confDir.listFiles(new FilenameFilter() {

        @Override
        public boolean accept(File dir, String name) {
          // Implements a naming convention of ending with "mdl.xml"
          // but is careful not to pick up mdl.xml itself again.
          return name.endsWith(MDL_XML) && !name.equals(MDL_XML);
        }

      });

      if (confFiles != null) {
        for (File confFile : confFiles) 
          config.addResource(new Path(confFile.getAbsolutePath()));
      }
    }
  }

  public String get(String key) {
    return config.get(key);
  }

  public void put(String key, String value) {
    this.config.set(key, value);
  }

  public Iterator<?> iterator() {
    return this.config.iterator();
  }

  public HashMap<String, String> startWith(String key) {
    HashMap<String, String> transformer = new HashMap<String, String>();
    Iterator<?> entries = config.iterator();
    while (entries.hasNext()) {
      String entry = entries.next().toString();
      if (entry.startsWith(key)) {
        String[] metrics = entry.split("=");
        transformer.put(metrics[0], metrics[1]);
      }
    }
    return transformer;
  }

  public String[] findTableName(String tableName, long start, long end) {
    String[] tableNames = null;
    String tableType = "_week";
    long now = (new Date()).getTime();
    long timeWindow = end - start;
    long partitionSize = WEEK;
    boolean fallback = true;

    if (config.get("consolidator.table." + tableName) == null) {
      tableNames = new String[1];
      tableNames[0] = tableName;
      return tableNames;
    }
    if (timeWindow <= 0) {
      timeWindow = 1;
    }
    if (timeWindow > DECADE) {
      tableType = "_century";
      partitionSize = CENTURY;
    } else if (timeWindow > YEAR) {
      tableType = "_decade";
      partitionSize = DECADE;
    } else if (timeWindow > QUARTER) {
      tableType = "_year";
      partitionSize = YEAR;
    } else if (timeWindow > MONTH) {
      tableType = "_quarter";
      partitionSize = QUARTER;
    } else if (timeWindow > WEEK) {
      tableType = "_month";
      partitionSize = MONTH;
    } else {
      tableType = "_week";
      partitionSize = WEEK;
    }

    long currentPartition = now / partitionSize;
    long startPartition = start / partitionSize;
    long endPartition = end / partitionSize;
    while (fallback && partitionSize != CENTURY * 100) {
      // Check if the starting date is in the far distance from current time. If
      // it is, use down sampled data.
      if (startPartition + 2 < currentPartition) {
        fallback = true;
        if (partitionSize == DAY) {
          tableType = "_week";
          partitionSize = WEEK;
        } else if (partitionSize == WEEK) {
          tableType = "_month";
          partitionSize = MONTH;
        } else if (partitionSize == MONTH) {
          tableType = "_year";
          partitionSize = YEAR;
        } else if (partitionSize == YEAR) {
          tableType = "_decade";
          partitionSize = DECADE;
        } else if (partitionSize == DECADE) {
          tableType = "_century";
          partitionSize = CENTURY;
        } else {
          partitionSize = 100 * CENTURY;
        }
        currentPartition = now / partitionSize;
        startPartition = start / partitionSize;
        endPartition = end / partitionSize;
      } else {
        fallback = false;
      }
    }

    if (startPartition != endPartition) {
      int delta = (int) (endPartition - startPartition);
      tableNames = new String[delta + 1];
      for (int i = 0; i <= delta; i++) {
        long partition = startPartition + (long) i;
        tableNames[i] = tableName + "_" + partition + tableType;
      }
    } else {
      tableNames = new String[1];
      tableNames[0] = tableName + "_" + startPartition + tableType;
    }
    return tableNames;
  }

  public String[] findTableNameForCharts(String tableName, long start, long end) {
    String[] tableNames = null;
    String tableType = "_week";
    long now = (new Date()).getTime();
    long timeWindow = end - start;
    if (timeWindow > 60 * 60 * 1000) {
      timeWindow = timeWindow + 1;
    }
    long partitionSize = WEEK;
    boolean fallback = true;

    if (config.get("consolidator.table." + tableName) == null) {
      tableNames = new String[1];
      tableNames[0] = tableName;
      return tableNames;
    }

    if (timeWindow <= 0) {
      timeWindow = 1;
    }
    if (timeWindow > DECADE) {
      tableType = "_decade";
      partitionSize = CENTURY;
    } else if (timeWindow > YEAR) {
      tableType = "_decade";
      partitionSize = CENTURY;
    } else if (timeWindow > QUARTER) {
      tableType = "_decade";
      partitionSize = DECADE;
    } else if (timeWindow > MONTH) {
      tableType = "_year";
      partitionSize = YEAR;
    } else if (timeWindow > WEEK) {
      tableType = "_quarter";
      partitionSize = QUARTER;
    } else if (timeWindow > DAY) {
      tableType = "_month";
      partitionSize = MONTH;
    } else {
      tableType = "_week";
      partitionSize = WEEK;
    }


    long currentPartition = now / partitionSize;
    long startPartition = start / partitionSize;
    long endPartition = end / partitionSize;
    while (fallback && partitionSize != DECADE * 100) {
      // Check if the starting date is in the far distance from current time. If
      // it is, use down sampled data.
      if (startPartition + 2 < currentPartition) {
        fallback = true;
        if (partitionSize == DAY) {
          tableType = "_month";
          partitionSize = MONTH;
        } else if (partitionSize == WEEK) {
          tableType = "_quarter";
          partitionSize = QUARTER;
        } else if (partitionSize == MONTH) {
          tableType = "_year";
          partitionSize = YEAR;
        } else if (partitionSize == YEAR) {
          tableType = "_decade";
          partitionSize = DECADE;
        } else {
          partitionSize = CENTURY;
        }
        currentPartition = now / partitionSize;
        startPartition = start / partitionSize;
        endPartition = end / partitionSize;
      } else {
        fallback = false;
      }
    }

    if (startPartition != endPartition) {
      int delta = (int) (endPartition - startPartition);
      tableNames = new String[delta + 1];
      for (int i = 0; i <= delta; i++) {
        long partition = startPartition + (long) i;
        tableNames[i] = tableName + "_" + partition + tableType;
      }
    } else {
      tableNames = new String[1];
      tableNames[0] = tableName + "_" + startPartition + tableType;
    }

    return tableNames;
  }

  public static void main(String[] args) {
    DatabaseConfig dbc = new DatabaseConfig();
    String[] names = dbc.findTableName("system_metrics", 1216140020000L,
        1218645620000L);
    for (String n : names) {
      System.out.println("name:" + n);
    }
  }
}
