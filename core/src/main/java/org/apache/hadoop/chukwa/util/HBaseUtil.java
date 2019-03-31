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
package org.apache.hadoop.chukwa.util;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.hadoop.chukwa.extraction.hbase.AbstractProcessor;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

public class HBaseUtil {
  private static Logger LOG = Logger.getLogger(HBaseUtil.class);
  
  static MessageDigest md5 = null;
  static {
    try {
      md5 = MessageDigest.getInstance("md5");
    } catch (NoSuchAlgorithmException e) {
      LOG.warn(ExceptionUtil.getStackTrace(e));
    }
  }

  public HBaseUtil() throws NoSuchAlgorithmException {
  }

  public byte[] buildKey(long time, String metricGroup, String metric,
      String source) {
    String fullKey = new StringBuilder(metricGroup).append(".")
        .append(metric).toString();
    return buildKey(time, fullKey, source);
  }

  public static byte[] buildKey(long time, String primaryKey) {
    Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    c.setTimeInMillis(time);
    byte[] day = Integer.toString(c.get(Calendar.DAY_OF_YEAR)).getBytes(Charset.forName("UTF-8"));
    byte[] pk = getHash(primaryKey);
    byte[] key = new byte[14];
    System.arraycopy(day, 0, key, 0, day.length);
    System.arraycopy(pk, 0, key, 2, 6);
    return key;
  }
  
  public static byte[] buildKey(long time, String primaryKey, String source) {
    Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    c.setTimeInMillis(time);
    byte[] day = Integer.toString(c.get(Calendar.DAY_OF_YEAR)).getBytes(Charset.forName("UTF-8"));
    byte[] pk = getHash(primaryKey);
    byte[] src = getHash(source);
    byte[] key = new byte[14];
    System.arraycopy(day, 0, key, 0, day.length);
    System.arraycopy(pk, 0, key, 2, 6);
    System.arraycopy(src, 0, key, 8, 6);
    return key;
  }
  
  private static byte[] getHash(String key) {
    byte[] hash = new byte[6];
    System.arraycopy(md5.digest(key.getBytes(Charset.forName("UTF-8"))), 0, hash, 0, 6);
    return hash;
  }
}
