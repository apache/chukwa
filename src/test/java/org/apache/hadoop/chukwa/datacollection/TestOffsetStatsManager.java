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

import junit.framework.TestCase;

/**
 * Verifies that the stats manager calculates stats properly. This test will take
 * at least 13 seconds to run.
 */
public class TestOffsetStatsManager extends TestCase {

  org.apache.hadoop.chukwa.datacollection.OffsetStatsManager<DummyKey> statsManager = null;
  DummyKey dummyKey = new DummyKey();

  protected void setUp() throws Exception {
    statsManager = new OffsetStatsManager<DummyKey>();
    dummyKey = new DummyKey();
  }

  public void testCalcAverageRate() throws InterruptedException {

    // add roughly 1000 bytes per second for about 5 seconds
    for (int i = 0; i < 5; i++) {
      statsManager.addOffsetDataPoint(dummyKey, 1000 * i, System.currentTimeMillis());
      Thread.sleep(1000);
    }

    // calculate 5 second average
    double rate = statsManager.calcAverageRate(dummyKey, 5);
    assertTrue("Invalid average, expected about 1 byte/sec, found " + rate,
                 Math.abs(1000 - rate) < 1.5);
  }

  public void testCalcAverageRateStaleData() throws InterruptedException {

    // add offsets for about 5 seconds, but timestamp them 3 seconds ago to make
    // them stale
    for (int i = 0; i < 5; i++) {
      statsManager.addOffsetDataPoint(dummyKey, 1000 * i, System.currentTimeMillis() - 3000L);
      Thread.sleep(1000);
    }

    // calculate 5 second average
    double rate = statsManager.calcAverageRate(dummyKey, 5);
    assertEquals("Should have gotten a stale data response", -1.0, rate);
  }

  public void testCalcAverageRateNotEnoughData() throws InterruptedException {

    // add offsets for about 3 seconds
    for (int i = 0; i < 3; i++) {
      statsManager.addOffsetDataPoint(dummyKey, 1000 * i, System.currentTimeMillis());
      Thread.sleep(1000);
    }

    // calculate 5 second average
    double rate = statsManager.calcAverageRate(dummyKey, 5);
    assertEquals("Should have gotten a stale data response", -1.0, rate);
  }

  private static class DummyKey {}
}
