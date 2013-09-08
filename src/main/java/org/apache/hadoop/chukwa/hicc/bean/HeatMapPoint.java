package org.apache.hadoop.chukwa.hicc.bean;

import javax.xml.bind.annotation.XmlElement;

public class HeatMapPoint {
  @XmlElement
  public int x;
  @XmlElement
  public int y;
  @XmlElement
  public double count;

  HeatMapPoint() {
  }

  HeatMapPoint(int x, int y, double count) {
    this.x = x;
	this.y = y;
	this.count = count;
  }

  public HeatMapPoint get() {
	return this;
  }

  public void put(int x, int y, double count) {
	this.x = x;
	this.y = y;
	this.count = count;
  }
}
