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

package org.apache.hadoop.chukwa.extraction.hbase;

import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.apache.hadoop.chukwa.util.ExceptionUtil;

public class HadoopMetricsProcessor extends AbstractProcessor {
  
  static Logger LOG = Logger.getLogger(HadoopMetricsProcessor.class);
  static final String timestampField = "timestamp";
  static final String contextNameField = "contextName";
  static final String recordNameField = "recordName";
  static final String hostName = "Hostname";
  static final String processName = "ProcessName";
  static final byte[] cf = "t".getBytes(Charset.forName("UTF-8"));

  public HadoopMetricsProcessor() throws NoSuchAlgorithmException {
  }

  @Override
  protected void parse(byte[] recordEntry) throws Throwable {
    String body = new String(recordEntry, Charset.forName("UTF-8"));
    int start = 0;
    int end = 0;
    try {
      while(true) {
        start = body.indexOf('{', end);
        end = body.indexOf('}', start)+1;
        if (start == -1)
          break;

        JSONObject json = (JSONObject) JSONValue.parse(body.substring(start,end));

        time = ((Long) json.get(timestampField)).longValue();
        String contextName = (String) json.get(contextNameField);
        String recordName = (String) json.get(recordNameField);
        String src = ((String) json.get(hostName)).toLowerCase();
        if(json.get(processName)!=null) {
          src = new StringBuilder(src).append(":").append(json.get(processName)).toString();
        }
        for(Entry<String, Object> entry : (Set<Map.Entry>) json.entrySet()) {
          String keyName = entry.getKey();
          if (timestampField.intern() == keyName.intern()) {
            continue;
          } else if (contextNameField.intern() == keyName.intern()) {
            continue;
          } else if (recordNameField.intern() == keyName.intern()) {
            continue;
          } else if (hostName.intern() == keyName.intern()) {
            continue;
          } else if (processName.intern() == keyName.intern()) {
            continue;
          } else {
            if(json.get(keyName)!=null) {
              String v = entry.getValue().toString();
              String primaryKey = new StringBuilder(contextName).append(".")
                .append(recordName).append(".").append(keyName).toString();
              addRecord(time, primaryKey, src, v.getBytes(Charset.forName("UTF-8")), output);
            }
          }
        }
      }
    } catch(Exception e) {
      LOG.warn("Unparsable data:");
      LOG.warn(body);
      LOG.warn(ExceptionUtil.getStackTrace(e));
      // Skip unparsable data.
    }
  }

}
