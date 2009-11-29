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
      id = json.getString("id");
      views = json.getJSONArray("views");
      properties = json.getJSONObject("properties");
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
      this.properties = new JSONObject(buffer);
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
  }
  
  public String getPropertyValue(String key) throws JSONException {
    return this.properties.getString(key);
  }
  
  public void setId(String id) {
    this.id=id;  
  }

  public void setViews(JSONArray ja) {
    this.views=ja;
  }
  
  public void setProperties(JSONObject properties) {
    this.properties = properties;
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
