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
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.hadoop.chukwa.util.ExceptionUtil;

public class ColumnBean {
  private WidgetBean[] widgets;
  
  private static Log log = LogFactory.getLog(ColumnBean.class);
  
  public ColumnBean() {
  }
  
  public ColumnBean(JSONArray json) throws ParseException {
    try {
      widgets = new WidgetBean[json.length()];
      for(int i=0;i<json.length();i++) {
        widgets[i]=new WidgetBean(json.getJSONObject(i));
      }
    } catch (JSONException e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new ParseException(ExceptionUtil.getStackTrace(e), 0);
    }
  }
  
  @XmlElement
  public WidgetBean[] getWidgets() {
    return widgets;
  }
  
  public void setWidgets(WidgetBean[] ws) {
    widgets=ws;
  }
  
  public void update() {
    for(int i=0;i<widgets.length;i++) {
      widgets[i].update();
    }
  }
  
  public JSONArray deserialize() {
    JSONArray ja = new JSONArray();
    for(int i=0;i<widgets.length;i++) {
      ja.put(widgets[i].deserialize());
    }
    return ja;
  }
}
