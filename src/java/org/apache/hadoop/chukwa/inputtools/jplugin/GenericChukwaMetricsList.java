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

import java.io.StringReader;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class GenericChukwaMetricsList extends ChukwaMetricsList<ChukwaMetrics> {
  private static DocumentBuilderFactory factory;
  private static DocumentBuilder docBuilder;
  static {
    factory = DocumentBuilderFactory.newInstance();
    try {
      docBuilder = factory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      e.printStackTrace();
    }
  }
  
  public GenericChukwaMetricsList() {
  }
  
  public GenericChukwaMetricsList(String recType) {
    super(recType);
  }
  
  public void fromXml(String xml) throws Exception {
    InputSource is = new InputSource(new StringReader(xml));
    Document doc = docBuilder.parse(is);
    Element root = doc.getDocumentElement();
    setRecordType(root.getTagName());
    long timestamp = Long.parseLong(root.getAttribute("ts"));
    setTimestamp(timestamp);
    
    NodeList children = root.getChildNodes();
    for(int i=0; i<children.getLength(); i++) {
      if(!children.item(i).getNodeName().equals("Metrics")) {
        continue;
      }
      NamedNodeMap attrs = children.item(i).getAttributes();
      if(attrs == null) {
        continue;
      }
      
      GenericChukwaMetrics metrics = new GenericChukwaMetrics();
      for(int a=0; a<attrs.getLength(); a++) {
        Attr attr = (Attr) attrs.item(a);
        String name = attr.getName();
        String value = attr.getValue();
        if(name.equals("key")) {
          metrics.setKey(value);
        } else {
          metrics.put(name, value);
        }
      }
      addMetrics(metrics);
    }
  }

  @SuppressWarnings("serial")
  public static class GenericChukwaMetrics extends HashMap<String, String> implements ChukwaMetrics {
    private String key;
    
    @Override
    public HashMap<String, String> getAttributes() {
      return this;
    }

    @Override
    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }
  }
}
