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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.hadoop.chukwa.util.ExceptionUtil;

@XmlRootElement
@XmlType
public class ViewBean {
  private PagesBean[] pages;
  private String description;
  private String owner;
  
  private String name;
  private String permissionType;
  private static Log log = LogFactory.getLog(ViewBean.class);

  public ViewBean() {  
  }
  
  public ViewBean(JSONObject json) throws ParseException {
    try {
      if(json.has("description")) {
        this.description = json.getString("description");
      } else {
        this.description = "";
      }
      this.owner=json.getString("owner");
      this.name=json.getString("name");
      this.permissionType=json.getString("permissionType");
      int size = json.getJSONArray("pages").length();
      PagesBean[] pages = new PagesBean[size];
      for(int i=0;i<size;i++) {
        pages[i] = new PagesBean(json.getJSONArray("pages").getJSONObject(i));
      }
      this.pages=pages;
    } catch (JSONException e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new ParseException(ExceptionUtil.getStackTrace(e), 0);
    }
  }
  
  @XmlElement
  public PagesBean[] getPages() {
    return pages;
  }
  
  @XmlElement
  public int getPagesCount() {
    return pages.length;  
  }
  
  @XmlElement
  public String getDescription() {
    return this.description;
  }
  
  @XmlElement
  public String getOwner() {
    return this.owner;
  }
  
  @XmlElement
  public String getName() {
    return this.name;
  }
  
  @XmlElement
  public String getPermissionType() {
    return this.permissionType;
  }
  
  public void setPages(PagesBean[] pages) {
    this.pages = pages;
  }
  
  public void setDescription(String description) {
    this.description = description;
  }
  
  public void setOwner(String owner) {
    this.owner = owner;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public void setPermissionType(String permissionType) {
    this.permissionType = permissionType;
  }
  
  public void update() {
    if(this.pages!=null) {
      for(PagesBean page : pages) {
        page.update();
      }
    }
  }
  
  public String deserialize() {
    update();
    JSONObject json = new JSONObject();
    try {
      json.put("name", this.name);
      json.put("owner", this.owner);
      json.put("permissionType", this.permissionType);
      json.put("description", this.description);
      JSONArray ja = new JSONArray();
      for(int i=0;i<this.pages.length;i++) {
        ja.put(this.pages[i].deserialize());
      }
      json.put("pages", (JSONArray) ja);
    } catch (Exception e){
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return json.toString();
  }
}
