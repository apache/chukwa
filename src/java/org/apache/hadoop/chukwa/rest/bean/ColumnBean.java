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
