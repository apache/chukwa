package org.apache.hadoop.chukwa.rest.bean;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement
@XmlType(propOrder={"code", "message"})
public class ReturnCodeBean {
  public static int FAIL=0;
  public static int SUCCESS=1;
  private int code;
  private String message;
  
  public ReturnCodeBean() {
  }
  
  public ReturnCodeBean(int code, String message) {
    this.code=code;
    this.message=message;
  }
  
  @XmlElement
  public int getCode() {
    return code;
  }
  
  @XmlElement
  public String getMessage() {
    return message;  
  }
  
  public void setCode(int code) {
    this.code=code;
  }
  
  public void setMessage(String message) {
    this.message=message;
  }
}
