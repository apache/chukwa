package org.apache.hadoop.chukwa.rest.bean;

import java.text.ParseException;
import java.util.Collection;
import java.util.HashSet;

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
      name=json.getString("name");
      type=json.getString("type");
      if(json.has("value")) {
        if(json.get("value").getClass()==JSONArray.class) {
          JSONArray ja = json.getJSONArray("value");
          Collection<String> c = new HashSet<String>();
          for(int i = 0; i < ja.length(); i++) {
            c.add(ja.getString(i));
          }
          this.value = c;
        } else {
          Collection<String> c = new HashSet<String>();
          c.add(json.getString("value"));
          this.value = c;
        }        
      }
      if(json.has("label")) {
        label=json.getString("label");
      } else {
        label=json.getString("name");
      }
      if(json.get("type").toString().intern()=="custom".intern()) {
        control=json.getString("control");
      }
      if(json.has("callback")) {
        callback=json.getString("callback");
      }
      if(json.has("options")) {
        JSONArray aj = json.getJSONArray("options");
        options = new OptionBean[aj.length()];
        for(int i=0;i<aj.length();i++) {
          OptionBean o = new OptionBean(aj.getJSONObject(i));
          options[i]=o;
        }
      }
      if(json.has("edit")) {
        edit=json.getInt("edit");
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
          ja.put(s);
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
          ja.put(this.options[i].deserialize());          
        }
        json.put("options", ja);
      }
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return json;
  }
}
