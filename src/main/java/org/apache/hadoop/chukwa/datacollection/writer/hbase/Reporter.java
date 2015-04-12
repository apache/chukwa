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

package org.apache.hadoop.chukwa.datacollection.writer.hbase;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.json.simple.JSONObject;
import org.mortbay.log.Log;

public class Reporter {
  private ArrayList<Put> meta = new ArrayList<Put>();
  private MessageDigest md5 = null;

  public Reporter() throws NoSuchAlgorithmException {
    md5 = MessageDigest.getInstance("md5");
  }

  public void putSource(String type, String source) {
    byte[] value = getHash(source);
    JSONObject json = new JSONObject();

    try {
      json.put("sig", new String(value, "UTF-8"));
      json.put("type", "source");
    } catch (UnsupportedEncodingException e) {
      Log.warn("Error encoding metadata.");
      Log.warn(e);
    }
    put(type.getBytes(), source.getBytes(), json.toString().getBytes());

  }

  public void putMetric(String type, String metric) {
    String buf = new StringBuilder(type).append(".").append(metric).toString();
    byte[] pk = getHash(buf);

    JSONObject json = new JSONObject();
    try {
      json.put("sig", new String(pk, "UTF-8"));
      json.put("type", "metric");
    } catch (UnsupportedEncodingException e) {
      Log.warn("Error encoding metadata.");
      Log.warn(e);
    }
    put(type.getBytes(), metric.getBytes(), json.toString().getBytes());

  }

  public void put(String key, String source, String info) {
    put(key.getBytes(), source.getBytes(), info.getBytes());
  }

  public void put(byte[] key, byte[] source, byte[] info) {
    Put put = new Put(key);
    put.add("k".getBytes(), source, info);
    meta.add(put);
  }

  public void clear() {
    meta.clear();
  }

  public List<Put> getInfo() {
    return meta;
  }

  private byte[] getHash(String key) {
    byte[] hash = new byte[3];
    System.arraycopy(md5.digest(key.getBytes()), 0, hash, 0, 3);
    return hash;
  }

  public void putClusterName(String type, String clusterName) {
    byte[] value = getHash(clusterName);
    JSONObject json = new JSONObject();

    try {
      json.put("sig", new String(value, "UTF-8"));
      json.put("type", "cluster");
    } catch (UnsupportedEncodingException e) {
      Log.warn("Error encoding metadata.");
      Log.warn(e);
    }
    put(type.getBytes(), clusterName.getBytes(), json.toString().getBytes());
  }

}
