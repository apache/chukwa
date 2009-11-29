package org.apache.hadoop.chukwa.rest.bean;

import java.text.ParseException;

import javax.xml.bind.annotation.XmlElement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.hadoop.chukwa.util.ExceptionUtil;

public class OptionBean {
  private String label;
  private String value;
  private static Log log = LogFactory.getLog(OptionBean.class);

  public OptionBean() {  
  }
  
  public OptionBean(JSONObject json) throws ParseException {
    try {
      label = json.getString("label");
      value = json.getString("value");
    } catch (JSONException e) {
      throw new ParseException(ExceptionUtil.getStackTrace(e), 0);
    }
  }

  @XmlElement
  public String getLabel() {
    return label;
  }
  
  @XmlElement
  public String getValue() {
    return value;
  }
  
  public void setLabel(String label) {
    this.label=label;
  }
  
  public void setValue(String value) {
    this.value=value;
  }
  
  public void update() {
    
  }
  
  public JSONObject deserialize() {
    JSONObject json = new JSONObject();
    try {
      json.put("label", label);
      json.put("value", value);
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return json;
  }
}
