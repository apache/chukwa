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
package org.apache.hadoop.chukwa.datacollection.agent.rest;

import java.util.ArrayList;
import java.util.List;

public class Examples {
  public static final AdaptorConfig CREATE_ADAPTOR_SAMPLE = new AdaptorConfig();
  public static final AdaptorInfo ADAPTOR_STATUS_SAMPLE = new AdaptorInfo();
  public static final List<AdaptorAveragedRate> ADAPTOR_RATES = new ArrayList<AdaptorAveragedRate>();
  public static final AdaptorAveragedRate ADAPTOR_RATE_SAMPLE_PER_MINUTE = new AdaptorAveragedRate();
  public static final AdaptorAveragedRate ADAPTOR_RATE_SAMPLE_PER_FIVE_MINUTE = new AdaptorAveragedRate();
  public static final AdaptorAveragedRate ADAPTOR_RATE_SAMPLE_PER_TEN_MINUTE = new AdaptorAveragedRate();
  
  public static final AdaptorInfo SYS_ADAPTOR_STATUS_SAMPLE = new AdaptorInfo();
  public static final List<AdaptorAveragedRate> SYS_ADAPTOR_RATES = new ArrayList<AdaptorAveragedRate>();
  public static final AdaptorAveragedRate SYS_ADAPTOR_RATE_SAMPLE_PER_MINUTE = new AdaptorAveragedRate();
  public static final AdaptorAveragedRate SYS_ADAPTOR_RATE_SAMPLE_PER_FIVE_MINUTE = new AdaptorAveragedRate();
  public static final AdaptorAveragedRate SYS_ADAPTOR_RATE_SAMPLE_PER_TEN_MINUTE = new AdaptorAveragedRate();
  
  public static final AdaptorList ADAPTOR_LIST_SAMPLE = new AdaptorList();
  
  static {
    // Create adaptor Sample
    CREATE_ADAPTOR_SAMPLE.setDataType("JobSummary");
    CREATE_ADAPTOR_SAMPLE.setAdaptorClass("org.apache.hadoop.chukwa.datacollection.adaptor.SocketAdaptor");
    CREATE_ADAPTOR_SAMPLE.setAdaptorParams("9098");
    CREATE_ADAPTOR_SAMPLE.setOffset(0);
    
    // Adaptor Status Sample
    ADAPTOR_RATE_SAMPLE_PER_MINUTE.setRate(100.123);
    ADAPTOR_RATE_SAMPLE_PER_MINUTE.setIntervalInSeconds(60);
    ADAPTOR_RATE_SAMPLE_PER_FIVE_MINUTE.setRate(100.123);
    ADAPTOR_RATE_SAMPLE_PER_FIVE_MINUTE.setIntervalInSeconds(300);
    ADAPTOR_RATE_SAMPLE_PER_TEN_MINUTE.setRate(100.123);
    ADAPTOR_RATE_SAMPLE_PER_TEN_MINUTE.setIntervalInSeconds(600);
    ADAPTOR_RATES.add(ADAPTOR_RATE_SAMPLE_PER_MINUTE);
    ADAPTOR_RATES.add(ADAPTOR_RATE_SAMPLE_PER_FIVE_MINUTE);
    ADAPTOR_RATES.add(ADAPTOR_RATE_SAMPLE_PER_TEN_MINUTE);
    ADAPTOR_STATUS_SAMPLE.setId("adaptor_93df4746476c9a4b624f6755b122f9dc");
    ADAPTOR_STATUS_SAMPLE.setDataType("JobSummary");
    ADAPTOR_STATUS_SAMPLE.setAdaptorClass("org.apache.hadoop.chukwa.datacollection.adaptor.SocketAdaptor");
    ADAPTOR_STATUS_SAMPLE.setAdaptorParams("9098");
    ADAPTOR_STATUS_SAMPLE.setOffset(1680);
    ADAPTOR_STATUS_SAMPLE.setAdaptorRates(ADAPTOR_RATES);
    
    // System Adaptor Sample
    SYS_ADAPTOR_RATE_SAMPLE_PER_MINUTE.setRate(9.09);
    SYS_ADAPTOR_RATE_SAMPLE_PER_MINUTE.setIntervalInSeconds(60);
    SYS_ADAPTOR_RATE_SAMPLE_PER_FIVE_MINUTE.setRate(7.55);
    SYS_ADAPTOR_RATE_SAMPLE_PER_FIVE_MINUTE.setIntervalInSeconds(300);
    SYS_ADAPTOR_RATE_SAMPLE_PER_TEN_MINUTE.setRate(6.44);
    SYS_ADAPTOR_RATE_SAMPLE_PER_TEN_MINUTE.setIntervalInSeconds(600);
    SYS_ADAPTOR_RATES.add(SYS_ADAPTOR_RATE_SAMPLE_PER_MINUTE);
    SYS_ADAPTOR_RATES.add(SYS_ADAPTOR_RATE_SAMPLE_PER_FIVE_MINUTE);
    SYS_ADAPTOR_RATES.add(SYS_ADAPTOR_RATE_SAMPLE_PER_TEN_MINUTE);
    SYS_ADAPTOR_STATUS_SAMPLE.setId("adaptor_c79bf882974a14286cffe29d3d4cf0d6");
    SYS_ADAPTOR_STATUS_SAMPLE.setDataType("SystemMetrics");
    SYS_ADAPTOR_STATUS_SAMPLE.setAdaptorClass("org.apache.hadoop.chukwa.datacollection.adaptor.sigar.SystemMetrics");
    SYS_ADAPTOR_STATUS_SAMPLE.setAdaptorParams("5");
    SYS_ADAPTOR_STATUS_SAMPLE.setOffset(5678);
    SYS_ADAPTOR_STATUS_SAMPLE.setAdaptorRates(SYS_ADAPTOR_RATES);
    
    // List of Adaptors Sample
    List<AdaptorInfo> list = new ArrayList<AdaptorInfo>();
    list.add(ADAPTOR_STATUS_SAMPLE);
    list.add(SYS_ADAPTOR_STATUS_SAMPLE);
    ADAPTOR_LIST_SAMPLE.setAdaptorInfo(list);
  }
}
