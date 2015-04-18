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


import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;

import org.apache.hadoop.chukwa.util.HBaseUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class HadoopMetricsProcessor extends AbstractProcessor {
  
  static Logger LOG = Logger.getLogger(HadoopMetricsProcessor.class);
  static final String timestampField = "timestamp";
  static final String contextNameField = "contextName";
  static final String recordNameField = "recordName";
  static final byte[] cf = "t".getBytes();

  public HadoopMetricsProcessor() throws NoSuchAlgorithmException {
  }

  @Override
  protected void parse(byte[] recordEntry) throws Throwable {
    try {
    	String body = new String(recordEntry);
        int start = body.indexOf('{');
        JSONObject json = (JSONObject) JSONValue.parse(body.substring(start));

        time = ((Long) json.get(timestampField)).longValue();
        String contextName = (String) json.get(contextNameField);
        String recordName = (String) json.get(recordNameField);
        byte[] timeInBytes = ByteBuffer.allocate(8).putLong(time).array();

        @SuppressWarnings("unchecked")
		Iterator<String> ki = json.keySet().iterator();
        while (ki.hasNext()) {
          String keyName = ki.next();
          if (timestampField.intern() == keyName.intern()) {
        	  continue;
          } else if (contextNameField.intern() == keyName.intern()) {
        	  continue;
          } else if (recordNameField.intern() == keyName.intern()) {
        	  continue;
          } else {
            if(json.get(keyName)!=null) {
                byte[] v = json.get(keyName).toString().getBytes();
                String primaryKey = new StringBuilder(contextName).append(".").
              		  append(recordName).append(".").
              		  append(keyName).toString();
                byte[] rowKey = HBaseUtil.buildKey(time, primaryKey, chunk.getSource());
                Put r = new Put(rowKey);
                r.add(cf, timeInBytes, time, v);
                output.add(r);
            }
          }
        }
        
      } catch (Exception e) {
        LOG.warn("Wrong format in HadoopMetricsProcessor [" + recordEntry + "]",
            e);
        throw e;
      }	
  }

}
