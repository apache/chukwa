package org.apache.hadoop.chukwa.rest.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@XmlType(propOrder={"type", "label", "children"})
public class CategoryBean {
  private static Log log = LogFactory.getLog(CategoryBean.class);
  private List<CategoryBean> children = new ArrayList<CategoryBean>();
  private String type = "text";
  private String label = null;
  
  public CategoryBean() {
  }
  
  @XmlElement
  public String getType() {
    return type;
  }
  
  @XmlElement
  public String getLabel() {
    return label;
  }
  
  @XmlElement
  public List<CategoryBean> getChildren() {
    return children;
  }
  
  public void setType(String type) {
    this.type = type;  
  }
  
  public void setLabel(String label) {
    this.label = label;
  }
  
  public void setChildren(List<CategoryBean> children) {
    this.children = children;
  }  
}
