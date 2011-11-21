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
package org.apache.hadoop.chukwa.hicc;

import java.util.Date;
import java.util.Random;
import java.util.TreeMap;

import junit.framework.TestCase;

public class AreaCalculatorTest extends TestCase {

  public void testGetAreaTreeMapOfStringTreeMapOfStringDouble() {
    TreeMap<String, TreeMap<String, Double>> maps = new TreeMap<String, TreeMap<String, Double>>();
    maps.put("abc", getDots());
    maps.put("def", getDots());

    TreeMap<String, Double> areas = AreaCalculator.getAreas(maps);
    System.out.println("Area of 'abc': " + areas.get("abc"));
    System.out.println("Area of 'def': " + areas.get("def"));
  }

  public void testGetAreaTreeMapOfStringDouble() {
    TreeMap<String, Double> map = getDots();
    System.out.println("Area: " + AreaCalculator.getArea(map));
  }

  public void testGetAreaDoubleDoubleDoubleDouble() {
    Double area = AreaCalculator.getArea(1, 4, 2, 4);
    System.out.println(area);
    assertEquals(true, area > 3.99999 && area < 4.00001);
  }

  private TreeMap<String, Double> getDots() {
    TreeMap<String, Double> map = new TreeMap<String, Double>();
    long now = new Date().getTime();
    Random r = new Random(now);
    for (long i = 0; i < 4; i++) {
      double value = r.nextInt(10) + 2;
      System.out.println(now + ": " + value);
      map.put(now + "", value);
      now += 1000;
    }
    return map;
  }
}
