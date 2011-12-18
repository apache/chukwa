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
package org.apache.hadoop.chukwa.hicc.bean;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;

import org.apache.hadoop.chukwa.datastore.ChukwaHBaseStore;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder={})
public class Series {
  private JSONObject series;
  static Logger log = Logger.getLogger(Series.class);
  
  public Series(String name) {
    series = new JSONObject();
    try {
      series.put("name", name);
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
  }
  
  public void add(long x, double y) {
    try {
    if(!series.containsKey("data")) {
      series.put("data", new JSONArray());
    }
    JSONArray xy = new JSONArray();
    xy.add(x);
    xy.add(y);
    ((JSONArray)series.get("data")).add(xy);
    } catch(Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
  }
  
  public String toString() {
    return series.toString();
  }

  public Object toJSONObject() {
    return series;
  }
}
