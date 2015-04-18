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

import org.apache.hadoop.chukwa.util.HBaseUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

public class DefaultProcessor extends AbstractProcessor {
	
  public DefaultProcessor() throws NoSuchAlgorithmException {
	super();
	// TODO Auto-generated constructor stub
  }

static Logger LOG = Logger.getLogger(DefaultProcessor.class);

  @Override
  protected void parse(byte[] recordEntry) throws Throwable {
	  byte[] key = HBaseUtil.buildKey(time, chunk.getDataType(), chunk.getSource());
	  Put put = new Put(key);
	  byte[] timeInBytes = ByteBuffer.allocate(8).putLong(time).array();
	  put.add("t".getBytes(), timeInBytes, chunk.getData());
	  output.add(put);
	  JSONObject json = new JSONObject();
	  json.put("sig", key);
	  json.put("type", "unknown");
	  reporter.put(chunk.getDataType(), chunk.getSource(), json.toString());
  }

}
