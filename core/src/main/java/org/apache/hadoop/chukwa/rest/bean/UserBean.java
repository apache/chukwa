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

package org.apache.hadoop.chukwa.rest.bean;

import java.text.ParseException;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import org.apache.hadoop.chukwa.util.ExceptionUtil;

@XmlRootElement
@XmlType(propOrder={"id", "views", "properties"})
public class UserBean {
  private String id;
  private JSONArray views;
  private JSONObject properties;
  private static Log log = LogFactory.getLog(UserBean.class);
  
  public UserBean() {
    views = new JSONArray();
    properties = new JSONObject();
  }
  
  public UserBean(JSONObject json) throws ParseException {
    try {
      id = (String) json.get("id");
      views = (JSONArray) json.get("views");
      if(json.containsKey("properties")) {
        properties = (JSONObject) json.get("properties");
      } else {
        properties = new JSONObject();
      }
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new ParseException("Error parsing user object.",0);
    }
  }
  
  @XmlElement
  public String getId() {
    return id;
  }
  
  @XmlElement
  public JSONArray getViews() {
    return views;
  }
  
  @XmlElement
  public String getProperties() {
    return properties.toString();
  }

  public void setProperties(String buffer) {
    try {
      this.properties = (JSONObject) JSONValue.parse(buffer);
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
  }
  
  public String getPropertyValue(String key) {
    return (String) this.properties.get(key);
  }
  
  public void setId(String id) {
    this.id=id;  
  }

  public void setViews(JSONArray ja) {
    this.views=ja;
  }

  public void setProperty(String key, String value) throws ParseException {
    try {
      this.properties.put(key, value);
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new ParseException("Error parsing user object.",0);      
    }
  }
  
  public JSONObject deserialize() {
    JSONObject json = new JSONObject();
    try {
      json.put("id", this.id);
      json.put("views", this.views);
      json.put("properties", this.properties);
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return json;
  }
}
