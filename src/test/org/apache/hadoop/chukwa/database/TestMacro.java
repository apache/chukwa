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
package org.apache.hadoop.chukwa.database;

import junit.framework.TestCase;
import java.util.TreeMap;
import java.util.ArrayList;

public class TestMacro extends TestCase {

  public void testPastXIntervals() {
    Macro m = new Macro(1234567890000L, "select '[past_5_minutes]';");
    System.out.println(m.toString());
    assertTrue(m.toString().intern()=="select '2009-02-13 15:25:00';".intern());
    m = new Macro(1234567890000L, "select '[past_hour]';");
    System.out.println(m.toString());
    assertTrue(m.toString().intern()=="select '2009-02-13 14:31:30';".intern());
    m = new Macro(1234567890000L, "select '[start]';");
    System.out.println(m.toString());
    assertTrue(m.toString().intern()=="select '2009-02-13 15:31:30';".intern());
  }

  public void testPartitions() {
    Macro m = new Macro(1234567890000L, "select from [system_metrics_week];");
    System.out.println(m.toString());
    assertTrue(m.toString().intern()=="select from system_metrics_2041_week;".intern());
    m = new Macro(1234567890000L, "select from [system_metrics_month];");
    System.out.println(m.toString());
    assertTrue(m.toString().intern()=="select from system_metrics_476_month;".intern());
    m = new Macro(1234567890000L, "select from [system_metrics_quarter];");
    System.out.println(m.toString());
    assertTrue(m.toString().intern()=="select from system_metrics_156_quarter;".intern());
    m = new Macro(1234567890000L, "select from [system_metrics_year];");
    System.out.println(m.toString());
    assertTrue(m.toString().intern()=="select from system_metrics_39_year;".intern());
    m = new Macro(1234567890000L, "select from [system_metrics_decade];");
    System.out.println(m.toString());
    assertTrue(m.toString().intern()=="select from system_metrics_3_decade;".intern());
  }

}
