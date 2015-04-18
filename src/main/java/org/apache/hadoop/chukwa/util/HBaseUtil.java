package org.apache.hadoop.chukwa.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.hadoop.chukwa.extraction.hbase.AbstractProcessor;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

public class HBaseUtil {
  private static Logger LOG = Logger.getLogger(HBaseUtil.class);
  
  static Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
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
    c.setTimeInMillis(time);
    byte[] day = Integer.toString(c.get(Calendar.DAY_OF_YEAR)).getBytes();
    byte[] pk = getHash(primaryKey);
    byte[] key = new byte[8];
    System.arraycopy(day, 0, key, 0, day.length);
    System.arraycopy(pk, 0, key, 2, 3);
    return key;
  }
  
  public static byte[] buildKey(long time, String primaryKey, String source) {
    c.setTimeInMillis(time);
    byte[] day = Integer.toString(c.get(Calendar.DAY_OF_YEAR)).getBytes();
    byte[] pk = getHash(primaryKey);
    byte[] src = getHash(source);
    byte[] key = new byte[8];
    System.arraycopy(day, 0, key, 0, day.length);
    System.arraycopy(pk, 0, key, 2, 3);
    System.arraycopy(src, 0, key, 5, 3);
    return key;
  }
  
  private static byte[] getHash(String key) {
    byte[] hash = new byte[3];
    System.arraycopy(md5.digest(key.getBytes()), 0, hash, 0, 3);
    return hash;
  }
}
