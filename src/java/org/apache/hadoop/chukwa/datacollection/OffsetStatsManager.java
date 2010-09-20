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
package org.apache.hadoop.chukwa.datacollection;

import org.apache.log4j.Logger;

import java.util.Map;
import java.util.LinkedList;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages stats for multiple objects of type T. T can be any class that is used
 * as a key for offset statistics (i.e. Agent, Collector, etc.). A client would
 * create an instance of this class and call <code>addOffsetDataPoint<code>
 * repeatedly over time. Then <code>calcAverageRate</code> can be called to
 * retrieve the average offset-unit per second over a given time interval.
 * <P>
 * For a given object T that is actively adding data points, stats are kept for
 * up to 20 minutes.
 * <P>
 * Care should be taken to always call <code>remove()</code> when old T objects
 * should no longer be tracked.
 */
public class OffsetStatsManager<T> {
  protected Logger log = Logger.getLogger(getClass());

  /*
   * This value is how far back we keep data for. Old data is purge when new
   * data is added.
   */
  private static long DEFAULT_STATS_DATA_TTL = 20L * 60L * 1000L; // 20 minutes

  /**
   * How far back can our data be to be considered fresh enough, relative to the
   * interval requests. For example if this value is 0.25 and interval requested
   * is 60 seconds, our most recent data point must be no more than 15 seconds old.
   */
  private static double DEFAULT_STALE_THRESHOLD = 0.25;


  /**
   * How far back do we need to have historical data for, relative to the
   * interval requested. For example if this value is 0.25 and the interval
   * requested is 60 seconds, our most oldest data point must be within 15
   * seconds of the most recent data point - 60.
   */
  private static double DEFAULT_AGE_THRESHOLD = 0.25;

  // These can be made configurable if someone needs to do so
  private long statsDataTTL = DEFAULT_STATS_DATA_TTL;
  private double staleThresholdPercent = DEFAULT_STALE_THRESHOLD;
  private double ageThresholdPercent = DEFAULT_AGE_THRESHOLD;

  private Map<T, OffsetDataStats> offsetStatsMap =
          new ConcurrentHashMap<T, OffsetDataStats>();

  public OffsetStatsManager() {
    this(DEFAULT_STATS_DATA_TTL);
  }

  public OffsetStatsManager(long statsDataTTL) {
    this.statsDataTTL = statsDataTTL;
  }

  /**
   * Record that at a given point in time an object key had a given offset.
   * @param key Object to key this data point to
   * @param offset How much of an offset to record
   * @param timestamp The time the offset occured
   */
  public void addOffsetDataPoint(T key, long offset, long timestamp) {
    OffsetDataStats stats = null;

    synchronized (offsetStatsMap) {
      if (offsetStatsMap.get(key) == null)
        offsetStatsMap.put(key, new OffsetDataStats());

      stats = offsetStatsMap.get(key);
    }

    stats.add(new OffsetData(offset, timestamp));
    stats.prune(statsDataTTL);

    if (log.isDebugEnabled())
      log.debug("Added offset - key=" + key + ", offset=" + offset +
                ", time=" + new Date(timestamp) + ", dataCount=" +
                stats.getOffsetDataList().size());
  }

  public double calcAverageRate(T key, long timeIntervalSecs) {
    OffsetDataStats stats = get(key);
    if (stats == null) {
      if (log.isDebugEnabled())
        log.debug("No stats data found key=" + key);
      return -1;
    }

    // first get the most recent data point to see if we're stale
    long now = System.currentTimeMillis();
    long mostRecentThreashold = now -
            timeIntervalSecs * (long)(staleThresholdPercent * 1000);
    OffsetData newestOffsetData = stats.mostRecentDataPoint();

    if (newestOffsetData == null || newestOffsetData.olderThan(mostRecentThreashold)) {
      if (log.isDebugEnabled())
        log.debug("Stats data too stale for key=" + key);

      return -1; // data is too stale
    }

    // then get the oldest data point to see if we have enough coverage
    long then = newestOffsetData.getTimestamp() - timeIntervalSecs * 1000L;
    long thenDelta = timeIntervalSecs * (long)(ageThresholdPercent * 1000);

    OffsetData oldestOffsetData = null;
    long minDiff = -1;
    long lastDiff = -1;
    for (OffsetData offsetData : stats.getOffsetDataList()) {
      long diff = offsetData.within(then, thenDelta);

      if (diff < 0) continue;

      if (minDiff == -1 || minDiff < diff) {
        // this is the data point closest to our target then time
        minDiff = diff;
        oldestOffsetData = offsetData;
      }

      // optimize so is we got a minDiff, but the diffs are getting worse, then
      // we've found the closet point and we can move on
      if (minDiff != -1 && lastDiff != -1 && diff > lastDiff) {
        break;
      }

      lastDiff = diff;
    }

    if (oldestOffsetData == null) {
      if (log.isDebugEnabled())
        log.debug("Stats data history too short for key=" + key);

      return -1;
    }

    return newestOffsetData.averageRate(oldestOffsetData);
  }

  public OffsetData oldestDataPoint(T key) {
    OffsetDataStats stats = get(key);
    return stats.oldestDataPoint();
  }

  public OffsetData mostRecentDataPoint(T key) {
    OffsetDataStats stats = get(key);
    return stats.mostRecentDataPoint();
  }

  /**
   * Remove key from the set of objects that we're tracking stats for.
   * @param key key of stats to be removed
   */
  public void remove(T key) {
    synchronized (offsetStatsMap) {
      offsetStatsMap.remove(key);
    }
  }

  /**
   * Remove all objectst that we're tracking stats for.
   */
  public void clear() {
    synchronized (offsetStatsMap) {
      offsetStatsMap.clear();
    }
  }

  /**
   * Fetch OffsetDataStats for key.
   * @param key key that stats are to be returned for
   */
  private OffsetDataStats get(T key) {
    synchronized (offsetStatsMap) {
      return offsetStatsMap.get(key);
    }
  }

  public class OffsetData {
    private long offset;
    private long timestamp;

    private OffsetData(long offset, long timestamp) {
      this.offset = offset;
      this.timestamp = timestamp;
    }

    public long getOffset() { return offset; }
    public long getTimestamp() { return timestamp; }

    public double averageRate(OffsetData previous) {
      if (previous == null) return -1;

      return new Double((offset - previous.getOffset())) /
             new Double((timestamp - previous.getTimestamp())) * 1000L;
    }

    public boolean olderThan(long timestamp) {
      return this.timestamp < timestamp;
    }

    public long within(long timestamp, long delta) {

      long diff = Math.abs(this.timestamp - timestamp);

      if (diff < delta) return diff;
      return -1;
    }
  }

  private class OffsetDataStats {
    private volatile LinkedList<OffsetData> offsetDataList = new LinkedList<OffsetData>();

    public LinkedList<OffsetData> getOffsetDataList() {
      return offsetDataList;
    }

    public void add(OffsetData offsetData) {
      synchronized(offsetDataList) {
        offsetDataList.add(offsetData);
      }
    }

    public OffsetData oldestDataPoint() {
      synchronized(offsetDataList) {
        return offsetDataList.peekFirst();
      }
    }

    public OffsetData mostRecentDataPoint() {
      synchronized(offsetDataList) {
        return offsetDataList.peekLast();
      }
    }

    public void prune(long ttl) {
      long cutoff = System.currentTimeMillis() - ttl;

      OffsetData data;
      synchronized(offsetDataList) {
        while ((data = offsetDataList.peekFirst()) != null) {
          if (data.getTimestamp() > cutoff) break;

          offsetDataList.removeFirst();
        }
      }
    }
  }
}
