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

import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.mortbay.log.Log;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class Reporter {
  private ArrayList<Put> meta = new ArrayList<Put>();
  private MessageDigest md5 = null;
  private final static Charset UTF8 = Charset.forName("UTF-8");

  public Reporter() throws NoSuchAlgorithmException {
    md5 = MessageDigest.getInstance("md5");
  }

  public void putSource(String type, String source) {
    byte[] value = getHash(source);
    String buffer;

    try {
      Type metaType = new TypeToken<Map<String, String>>(){}.getType();
      Map<String, String> meta = new HashMap<String, String>();
      meta.put("sig", new String(value, UTF8));
      meta.put("type", "source");
      Gson gson = new Gson();
      buffer = gson.toJson(meta, metaType);
      put(type.getBytes(UTF8), source.getBytes(UTF8), buffer.toString().getBytes(UTF8));
    } catch (Exception e) {
      Log.warn("Error encoding metadata.");
      Log.warn(e);
    }
  }

  public void putMetric(String type, String metric) {
    String buf = new StringBuilder(type).append(".").append(metric).toString();
    byte[] pk = getHash(buf);
    String buffer;
    try {
      Type metaType = new TypeToken<Map<String, String>>(){}.getType();
      Map<String, String> meta = new HashMap<String, String>();
      meta.put("sig", new String(pk, "UTF-8"));
      meta.put("type", "metric");
      Gson gson = new Gson();
      buffer = gson.toJson(meta, metaType);
      put(type.getBytes(UTF8), metric.getBytes(UTF8), buffer.toString().getBytes(UTF8));
    } catch (Exception e) {
      Log.warn("Error encoding metadata.");
      Log.warn(e);
    }
  }

  public void put(String key, String source, String info) {
    put(key.getBytes(UTF8), source.getBytes(UTF8), info.getBytes(UTF8));
  }

  public void put(byte[] key, byte[] source, byte[] info) {
    Put put = new Put(key);
    put.addColumn("k".getBytes(UTF8), source, info);
    meta.add(put);
  }

  public void clear() {
    meta.clear();
  }

  public List<Put> getInfo() {
    return meta;
  }

  private byte[] getHash(String key) {
    byte[] hash = new byte[5];
    System.arraycopy(md5.digest(key.getBytes(UTF8)), 0, hash, 0, 5);
    return hash;
  }

  public void putClusterName(String type, String clusterName) {
    byte[] value = getHash(clusterName);
    String buffer;
    Type metaType = new TypeToken<Map<String, String>>(){}.getType();
    Map<String, String> meta = new HashMap<String, String>();
    meta.put("sig", new String(value, UTF8));
    meta.put("type", "cluster");
    Gson gson = new Gson();
    buffer = gson.toJson(meta, metaType);
    put(type.getBytes(UTF8), clusterName.getBytes(UTF8), buffer.toString().getBytes(UTF8));
  }

}
