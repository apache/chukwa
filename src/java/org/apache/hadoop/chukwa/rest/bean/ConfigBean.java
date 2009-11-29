package org.apache.hadoop.chukwa.rest.bean;

import java.text.ParseException;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import org.apache.hadoop.chukwa.util.ExceptionUtil;

@XmlRootElement
@XmlType(propOrder={"key", "value"})
public class ConfigBean {
  private String key = null;
  private String value = null;
  private static Log log = LogFactory.getLog(ViewBean.class);
  
  public ConfigBean() {
  }
  
  public ConfigBean(JSONObject json) throws ParseException {
    try {
      key = json.getString("key");
      value = json.getString("value");
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new ParseException("Error parsing user object.",0);
    }
  }
  
  @XmlElement
  public String getKey() {
    return key;
  }
  
  @XmlElement
  public String getValue() {
    return value;
  }
  
  public void setKey(String key) {
    this.key = key;
  }
  
  public void setValue(String value) {
    this.value = value;
  }
}
