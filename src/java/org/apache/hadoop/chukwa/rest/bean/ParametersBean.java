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
import java.util.HashSet;

import javax.xml.bind.annotation.XmlElement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.hadoop.chukwa.util.ExceptionUtil;

public class ParametersBean {
  private static Log log = LogFactory.getLog(ParametersBean.class);
  private String name=null;
  private String type=null;
  private Collection<String> value=null;
  private String control=null;
  private String label=null;
  private String callback=null;
  private int edit=1;
  private OptionBean[] options=null;
  
  public ParametersBean() {    
  }
  
  public ParametersBean(JSONObject json) throws ParseException {
    try {
      name=(String) json.get("name");
      type=(String) json.get("type");
      if(json.containsKey("value")) {
        if(json.get("value").getClass()==JSONArray.class) {
          JSONArray ja = (JSONArray) json.get("value");
          Collection<String> c = new HashSet<String>();
          for(int i = 0; i < ja.size(); i++) {
            c.add((String) ja.get(i));
          }
          this.value = c;
        } else {
          Collection<String> c = new HashSet<String>();
          c.add((String)json.get("value"));
          this.value = c;
        }        
      }
      if(json.containsKey("label")) {
        label=(String) json.get("label");
      } else {
        label=(String) json.get("name");
      }
      if(json.get("type").toString().intern()=="custom".intern()) {
        control=(String) json.get("control");
      }
      if(json.containsKey("callback")) {
        callback=(String) json.get("callback");
      }
      if(json.containsKey("options")) {
        JSONArray aj = (JSONArray) json.get("options");
        options = new OptionBean[aj.size()];
        for(int i=0;i<aj.size();i++) {
          OptionBean o = new OptionBean((JSONObject) aj.get(i));
          options[i]=o;
        }
      }
      if(json.containsKey("edit")) {
        if(json.get("edit").getClass().equals(String.class)) {
          edit=(new Integer((String)json.get("edit"))).intValue();          
        } else if(json.get("edit").getClass().equals(Long.class)) {
          edit=((Long)json.get("edit")).intValue();          
        }
      }
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new ParseException(ExceptionUtil.getStackTrace(e), 0);
    }
  }
  
  @XmlElement
  public String getName() {
    return name;
  }
  
  @XmlElement
  public String getType() {
    return type;
  }

  @XmlElement
  public Collection<String> getValue() {
    return value;
  }

  @XmlElement
  public String getControl() {
    return control;  
  }
  
  @XmlElement
  public String getLabel() {
    return label;  
  }
  
  @XmlElement
  public OptionBean[] getOptions() {
   return options; 
  }
  
  @XmlElement
  public int getEdit() {
    return edit;
  }

  @XmlElement
  public String getCallback() {
    return callback;
  }

  public void setName(String name) {
    this.name = name;
  }
  
  public void setType(String type) {
    this.type = type;
  }

  public void setValue(Collection<String> value) {
    this.value = value;
  }
  
  public void setControl(String control) {
    this.control = control;
  }
  
  public void setLabel(String label) {
    this.label = label;
  }
  
  public void setOptions(OptionBean[] options) {
    this.options = options;
  }
  
  public void setEdit(int edit) {
    this.edit = edit;
  }
  
  public void setCallback(String callback) {
    this.callback = callback;  
  }

  public JSONObject deserialize() {
    JSONObject json = new JSONObject();
    try {
      json.put("name",this.name);
      json.put("type",this.type);
      if(this.value!=null) {
        JSONArray ja = new JSONArray();
        for(String s : this.value) {
          ja.add(s);
        }
        json.put("value", ja);
      }
      if(control!=null) {
        json.put("control",this.control);
      }
      json.put("label",this.label);
      json.put("edit",this.edit);
      if(this.callback!=null) {
        json.put("callback", callback);
      }
      if(options!=null) {
        JSONArray ja = new JSONArray();
        for(int i=0;i<options.length;i++) {
          ja.add(this.options[i].deserialize());          
        }
        json.put("options", ja);
      }
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return json;
  }
}
