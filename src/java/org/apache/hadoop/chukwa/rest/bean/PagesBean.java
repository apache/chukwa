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
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.hadoop.chukwa.util.ExceptionUtil;

public class PagesBean {
  private static Log log = LogFactory.getLog(PagesBean.class);
  private String title;
  private int[] columnSizes;
  private ColumnBean[] layout;
  private int columns;
  
  public PagesBean() {
  }

  
  public PagesBean(JSONObject json) throws ParseException {
    try {
      title = (String) json.get("title");
      columns = ((Long) json.get("columns")).intValue();
      JSONArray layout = (JSONArray) json.get("layout");
      this.layout = new ColumnBean[layout.size()];
      for(int i=0;i<layout.size();i++) {
        ColumnBean c = new ColumnBean((JSONArray) layout.get(i));
        this.layout[i]=c;
      }
      if(json.containsKey("colSize")) {
        JSONArray ja = (JSONArray) json.get("colSize");
        columnSizes = new int[ja.size()];
        for(int i=0; i< ja.size(); i++) {
          columnSizes[i] = ((Long) ja.get(i)).intValue();
        }
      }
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new ParseException(ExceptionUtil.getStackTrace(e), 0);
    }
  }
  
  @XmlElement
  public String getTitle() {
    return title;
  }

//  @XmlElement
//  public String getLayoutStyle() {
//    return layoutStyle;
//  }

  @XmlElement(name="layout")
  public ColumnBean[] getLayout() {
    return layout;
  }
  
  public void update() {
    for(int i=0;i<layout.length;i++) {
      layout[i].update();
    }
  }
  
  public void setTitle(String title) {
    this.title = title;
  }

  public void setLayout(ColumnBean[] layout) {
    this.layout = layout;
  }

  @XmlElement(name="colSize")
  public int[] getColSize() {
    return this.columnSizes;  
  }
  
  public void setColSize(int[] size) {
    this.columnSizes = size;
  }

  @XmlElement(name="columns")
  public int getColumns() {
    return this.columns;
  }
  
  public void setColumns(int columns) {
    this.columns = columns;
  }
  
  public JSONObject deserialize() {
    JSONObject json = new JSONObject();
    JSONArray ja = new JSONArray();
    JSONArray sizes = new JSONArray();
    try {
      json.put("title", this.title);
      for(int i=0;i<layout.length;i++) {
        ja.add(layout[i].deserialize());
      }
      json.put("layout", (JSONArray) ja);
      json.put("columns", layout.length);
      if(columnSizes!=null) {
        for(int colSize : columnSizes) {
          sizes.add(colSize);
        }
      }
      json.put("colSize", (JSONArray) sizes);
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return json;
  }

}
