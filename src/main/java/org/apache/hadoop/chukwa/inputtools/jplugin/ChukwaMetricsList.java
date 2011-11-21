/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.chukwa.inputtools.jplugin;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ChukwaMetricsList<T extends ChukwaMetrics> {
  private ArrayList<T> metricsList = null;
  private long timestamp = new Date().getTime();
  private String recordType = "JPlugin";

  public ChukwaMetricsList() {
  }
  
  public ChukwaMetricsList(String recordType) {
    setRecordType(recordType);
  }
  
  public void setMetricsList(ArrayList<T> metricsList) {
    this.metricsList = metricsList;
  }

  public ArrayList<T> getMetricsList() {
    if(metricsList == null){
      metricsList = new ArrayList<T>();
    }
    return metricsList;
  }

  public void addMetrics(T metrics) {
    getMetricsList().add(metrics);
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
  
  public String toXml() throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder docBuilder = null;
    docBuilder = factory.newDocumentBuilder();
    Document doc = docBuilder.newDocument();
    Element root = doc.createElement(getRecordType());
    doc.appendChild(root);
    root.setAttribute("ts", getTimestamp()+"");
    for(ChukwaMetrics metrics : getMetricsList()) {
      Element elem = doc.createElement("Metrics");
      elem.setAttribute("key", metrics.getKey());
      for(Entry<String, String> attr : metrics.getAttributes().entrySet()) {
        elem.setAttribute(attr.getKey(), attr.getValue());
      }
      root.appendChild(elem);
    }
    
    Transformer transformer = TransformerFactory.newInstance().newTransformer();
    transformer.setOutputProperty("indent", "yes");
    StringWriter sw = new StringWriter();
    transformer.transform(new DOMSource(doc), new StreamResult(sw));
    
    return sw.toString();
  }

  public void setRecordType(String recordType) {
    this.recordType = recordType;
  }

  public String getRecordType() {
    return recordType;
  }
  
}
