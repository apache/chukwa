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

import java.util.TreeMap;
import java.util.Map.Entry;

public class AreaCalculator {
  public static TreeMap<String, Double> getAreas(
      TreeMap<String, TreeMap<String, Double>> dataSet) 
  {
    TreeMap<String, Double> areas = new TreeMap<String, Double>();
    for (Entry<String, TreeMap<String, Double>> entry : dataSet.entrySet()) {
      String key = entry.getKey();
      Double area = getArea(entry.getValue());
      areas.put(key, area);
    }
    return areas;
  }

  public static Double getArea(TreeMap<String, Double> data) {
    double area = 0;
    boolean first = true;
    double x0, x1, y0, y1;
    x0 = x1 = y0 = y1 = 0;
    for (Entry<String, Double> entry : data.entrySet()) {
      double x = Double.parseDouble(entry.getKey());
      double y = entry.getValue();
      if (first) {
        x0 = x;
        y0 = y;
        first = false;
      } else {
        x1 = x;
        y1 = y;
        area += getArea(x0, y0, x1, y1);
        x0 = x1;
        y0 = y1;
      }
    }
    return area;
  }

  public static Double getArea(double x0, double y0, double x1, double y1) {
    return (x1 - x0) * (y0 + y1) / 2;
  }
}
