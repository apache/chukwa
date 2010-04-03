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
import java.util.Collection;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.hadoop.chukwa.datastore.WidgetStore;
import org.apache.hadoop.chukwa.util.ExceptionUtil;

@XmlRootElement
@XmlType(propOrder={"id", "title", "version", "categories", "url", "description","refresh","parameters"})
public class WidgetBean {
  private String id;
  private String title;
  private String version;
  private String categories;
  private String url;
  private String description;
  private int refresh;
  private ParametersBean[] parameters;
  private static Log log = LogFactory.getLog(WidgetBean.class);

  public WidgetBean() {
    
  }
  
  public WidgetBean(JSONObject json) throws ParseException {
    try {
      this.id=json.getString("id");
      this.title=json.getString("title");
      this.version=json.getString("version");
      this.categories=json.getString("categories");
      this.url=json.getString("url");
      this.description=json.getString("description");
      this.refresh=json.getInt("refresh");
      try {
        int size = json.getJSONArray("parameters").length();
        ParametersBean[] list = new ParametersBean[size];
        for(int i=0;i<size;i++) {
          list[i] = new ParametersBean(json.getJSONArray("parameters").getJSONObject(i));
        }
        this.parameters=list;
      } catch (JSONException e) {
        this.parameters=null;
      }
    } catch (JSONException e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new ParseException(ExceptionUtil.getStackTrace(e), 0);
    }
  }
  
  @XmlElement
  public String getId() {
    return id;
  }

  @XmlElement
  public String getTitle() {
    return title;
  }
  
  @XmlElement
  public String getVersion() {
    return version;
  }
  
  @XmlElement
  public String getCategories() {
    return categories;
  }
  
  @XmlElement
  public String getUrl() {
    return url;
  }
  
  @XmlElement
  public String getDescription() {
    return description;
  }
  
  @XmlElement
  public int getRefresh() {
    return refresh;
  }
  
  @XmlElement
  public ParametersBean[] getParameters() {
    return parameters;
  }
  
  public void setId(String id) {
    this.id=id;
  }
  
  public void setUrl(String url) {
    this.url=url;
  }
  
  public void setTitle(String title) {
    this.title=title;
  }
  
  public void setDescription(String description) {
    this.description=description;
  }
  
  public void setVersion(String version) {
    this.version=version;
  }
  
  public void setCategories(String categories) {
    this.categories=categories;
  }
  
  public void setRefresh(int refresh) {
    this.refresh=refresh;
  }
  
  public void setParameters(ParametersBean[] p) {
    this.parameters=p;
  }
  
  public void update() {
    try {
      WidgetBean widget = WidgetStore.list().get(this.id);
      if(widget!=null) {
        if(widget.getVersion().intern()!=this.version.intern()) {
          this.categories=widget.getCategories();
          this.title=widget.getTitle();
          this.version=widget.getVersion();
          this.url=widget.getUrl();
          this.description=widget.getDescription();
          ParametersBean[] plist = widget.getParameters();
          for(int i=0;i<this.parameters.length;i++) {
            Collection<String> value = this.parameters[i].getValue();
            for(int j=0;j<plist.length;j++) {
              if(plist[i].getName().intern()==this.parameters[j].getName().intern()) {
                plist[j].setValue(value);
              }
            }
          }
          this.parameters=plist;
        }
      } else {
        log.info("Widget "+this.id+" is deprecated.");
      }
    } catch (IllegalAccessException e) {
      log.error("Unable to update widget: "+this.id+" "+ExceptionUtil.getStackTrace(e));
    }
    
  }
  
  public JSONObject deserialize() {
    JSONObject json = new JSONObject();
    try {
      json.put("id", this.id);
      json.put("title", this.title);
      json.put("description", this.description);
      json.put("version", this.version);
      json.put("categories", this.categories);
      json.put("refresh",this.refresh);
      json.put("url", this.url);
      JSONArray ja = new JSONArray();
      if(this.parameters!=null) {
        for(int i=0;i<this.parameters.length;i++) {
          ja.put(this.parameters[i].deserialize());
        }
      }
      json.put("parameters", (JSONArray) ja);
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return json;    
  }
}
