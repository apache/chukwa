package org.apache.hadoop.chukwa.rest.bean;

import java.text.ParseException;

import javax.xml.bind.annotation.XmlElement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import org.apache.hadoop.chukwa.util.ExceptionUtil;

public class ParametersBean {
  private static Log log = LogFactory.getLog(ParametersBean.class);
  private String name=null;
  private String type=null;
  private String value=null;
  private String control=null;
  private String label=null;
  private OptionBean[] options=null;
  
  public ParametersBean() {    
  }
  
  public ParametersBean(JSONObject json) throws ParseException {
    try {
      name=json.getString("name");
      type=json.getString("type");
      value=json.getString("value");
      if(json.has("label")) {
        label=json.getString("label");
      } else {
        label=json.getString("name");
      }
      if(json.get("type").toString().intern()=="custom".intern()) {
        control=json.getString("control");
      }
      if(json.has("options")) {
        JSONArray aj = json.getJSONArray("options");
        options = new OptionBean[aj.length()];
        for(int i=0;i<aj.length();i++) {
          OptionBean o = new OptionBean(aj.getJSONObject(i));
          options[i]=o;
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
  public String getValue() {
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
  
  public void setName(String name) {
    this.name = name;
  }
  
  public void setType(String type) {
    this.type = type;
  }
  
  public void setValue(String value) {
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
  
  public JSONObject deserialize() {
    JSONObject json = new JSONObject();
    try {
      json.put("name",this.name);
      json.put("type",this.type);
      json.put("value",this.value);
      if(control!=null) {
        json.put("control",this.control);
      }
      json.put("label",this.label);
      if(options!=null) {
        JSONArray ja = new JSONArray();
        for(int i=0;i<options.length;i++) {
          ja.put(this.options[i].deserialize());          
        }
        json.put("options", (JSONArray) ja);
      }
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return json;
  }
}
