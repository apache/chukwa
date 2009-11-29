package org.apache.hadoop.chukwa.rest.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@XmlRootElement
@XmlType(propOrder={"type", "id", "label", "children"})
public class CatalogBean {
  private static Log log = LogFactory.getLog(CatalogBean.class);
  private List<CatalogBean> children = new ArrayList<CatalogBean>();
  private String type = "text";
  private String label = null;
  private String id = null;
  
  public CatalogBean() {
  }
  
  @XmlElement
  public String getType() {
    return type;
  }
  
  @XmlElement
  public String getId() {
    return id;
  }
  
  @XmlElement
  public String getLabel() {
    return label;
  }
  
  @XmlElement
  public List<CatalogBean> getChildren() {
    return children;
  }
  
  public void setType(String type) {
    this.type = type;  
  }
  
  public void setId(String id) {
    this.id = id;
  }
  
  public void setLabel(String label) {
    this.label = label;
  }
  
  public void setChildren(List<CatalogBean> children) {
    this.children = children;
  }
  
  public void addCatalog(WidgetBean widget) {
    String[] path = widget.getCategories().split(",");
    List<CatalogBean> tracker = this.children;
    if(tracker==null) {
      tracker = new ArrayList<CatalogBean>();
    }
    for(int i=0;i<path.length;i++) {
      boolean duplicate = false;
      for(int j=0;j<tracker.size();j++) {
        if(tracker.get(j).getLabel().intern()==path[i].intern()) {
          duplicate = true;
          tracker = tracker.get(j).getChildren();
          continue;
        }
      }
      if(!duplicate) {
        tracker = addCategory(tracker, widget.getId(), path[i]);
      }
    }
    tracker = addCategory(tracker, widget.getId(), widget.getTitle());
  }
  
  public List<CatalogBean> addCategory(List<CatalogBean> tracker, String id, String label) {
    CatalogBean c = new CatalogBean();
    c.setId(id);
    c.setLabel(label);
    tracker.add(c);
    return c.getChildren();
  }
  
}
