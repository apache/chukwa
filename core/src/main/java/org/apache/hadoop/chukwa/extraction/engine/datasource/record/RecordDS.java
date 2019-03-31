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

package org.apache.hadoop.chukwa.extraction.engine.datasource.record;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.chukwa.extraction.engine.SearchResult;
import org.apache.hadoop.chukwa.extraction.engine.Token;
import org.apache.hadoop.chukwa.extraction.engine.datasource.DataSource;
import org.apache.hadoop.chukwa.extraction.engine.datasource.DataSourceException;
import org.apache.hadoop.chukwa.inputtools.mdl.DataConfig;
import org.apache.hadoop.fs.FileSystem;

public class RecordDS implements DataSource {

  private static FileSystem fs = null;
  private static ChukwaConfiguration conf = null;

  private static String rootFolder = null;
  private static DataConfig dataConfig = null;

  static {
    dataConfig = new DataConfig();
    rootFolder = dataConfig.get("chukwa.engine.dsDirectory.rootFolder");
    conf = new ChukwaConfiguration();
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public SearchResult search(SearchResult result, String cluster,
      String dataSource, long t0, long t1, String filter, Token token)
      throws DataSourceException {

    String filePath = rootFolder + "/" + cluster + "/" + dataSource;

    System.out.println("filePath [" + filePath + "]");
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(t1);

    TreeMap<Long, List<Record>> records = result.getRecords();
    int maxCount = 200;
    SimpleDateFormat sdf = new java.text.SimpleDateFormat("_yyyyMMdd_HH_");
    do {
      System.out.println("start Date [" + calendar.getTime() + "]");
      String fileName = sdf.format(calendar.getTime());
      int minutes = calendar.get(Calendar.MINUTE);
      int dec = minutes / 10;
      fileName += dec;

      int m = minutes - (dec * 10);
      if (m < 5) {
        fileName += "0.1.evt";
      } else {
        fileName += "5.1.evt";
      }

      fileName = filePath + "/" + dataSource + fileName;

      // System.out.println("JB fileName  [" +fileName + "]");

      try {
        System.out.println("BEFORE fileName  [" + fileName + "]");

        // List<Record> evts =
        // ChukwaFileParser.readData(cluster,dataSource,maxCount, t1, t0,
        // Long.MAX_VALUE, filter, fileName, fs);
        List<Record> evts = ChukwaSequenceFileParser.readData(cluster,
            dataSource, maxCount, t1, t0, Long.MAX_VALUE, filter, fileName, fs,
            conf);

        maxCount = maxCount - evts.size();
        System.out.println("AFTER fileName  [" + fileName + "] count="
            + evts.size() + " maxCount=" + maxCount);
        for (Record evt : evts) {
          System.out.println("AFTER Loop  [" + evt.toString() + "]");
          long timestamp = evt.getTime();
          if (records.containsKey(timestamp)) {
            records.get(timestamp).add(evt);
          } else {
            List<Record> list = new LinkedList<Record>();
            list.add(evt);
            records.put(timestamp, list);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }

      if (maxCount <= 0) {
        System.out.println("BREAKING LOOP AFTER [" + fileName + "] maxCount="
            + maxCount);
        break;
      }

      calendar.add(Calendar.MINUTE, -5);

      System.out.println("calendar  [" + calendar.getTimeInMillis() + "] ");
      System.out.println("end       [" + (t0 - 1000 * 60 * 5) + "] ");
    } while (calendar.getTimeInMillis() > (t0 - 1000 * 60 * 5)); // <= need some
                                                                 // code here
    // Need more than this to compute the end

    return result;
  }

  public boolean isThreadSafe() {
    return true;
  }
}
