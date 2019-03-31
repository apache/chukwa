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
package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

@Tables(annotations={
@Table(name="Mapreduce",columnFamily="JobData"),
@Table(name="Mapreduce",columnFamily="JobConfData")
})
public class JobConfProcessor extends AbstractProcessor {
    static Logger log = Logger.getLogger(JobConfProcessor.class);
    private static final String jobData = "JobData";
    private static final String jobConfData = "JobConfData";
    
    static  Pattern timePattern = Pattern.compile("(.*)?time=\"(.*?)\"(.*)?");
    static  Pattern jobPattern = Pattern.compile("(.*?)job_(.*?)_conf\\.xml(.*?)");
    @Override
    protected void parse(String recordEntry,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
      Reporter reporter) 
   throws Throwable
  {
    Long time = 0L;
    Random randomNumber = new Random();
    String tags = this.chunk.getTags();

    Matcher matcher = timePattern.matcher(tags);
    if (matcher.matches()) {
      time = Long.parseLong(matcher.group(2));
    }
    String capp = this.chunk.getStreamName();
      String jobID = "";
        matcher = jobPattern.matcher(capp);
        if(matcher.matches()) {
          jobID=matcher.group(2);
        }
        ChukwaRecord record = new ChukwaRecord();
        ChukwaRecord jobConfRecord = new ChukwaRecord();
      DocumentBuilderFactory docBuilderFactory 
        = DocumentBuilderFactory.newInstance();
      //ignore all comments inside the xml file
      docBuilderFactory.setIgnoringComments(true);
      FileOutputStream out = null;
      try {
          DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
          Document doc = null;
          String fileName = "test_"+randomNumber.nextInt();
          File tmp = new File(fileName);
          out = new FileOutputStream(tmp);
          out.write(recordEntry.getBytes(Charset.forName("UTF-8")));
          out.close();
        doc = builder.parse(fileName);
        Element root = doc.getDocumentElement();
        if (!"configuration".equals(root.getTagName()))
            log.fatal("bad conf file: top-level element not <configuration>");
        NodeList props = root.getChildNodes();
            JSONObject json = new JSONObject();
            String queue = "default";
    
        for (int i = 0; i < props.getLength(); i++) {
            Node propNode = props.item(i);
            if (!(propNode instanceof Element))
                continue;
            Element prop = (Element)propNode;
            if (!"property".equals(prop.getTagName()))
                log.warn("bad conf file: element not <property>");
            NodeList fields = prop.getChildNodes();
            String attr = null;
            String value = null;
            for (int j = 0; j < fields.getLength(); j++) {
                Node fieldNode = fields.item(j);
                if (!(fieldNode instanceof Element))
                    continue;
                Element field = (Element)fieldNode;
                if ("name".equals(field.getTagName()) && field.hasChildNodes())
                    attr = ((Text)field.getFirstChild()).getData().trim();
                if ("value".equals(field.getTagName()) && field.hasChildNodes())
                    value = ((Text)field.getFirstChild()).getData();
            }
            
            // Ignore this parameter if it has already been marked as 'final'
            if (attr != null && value != null) {
                json.put(attr, value);
                if(attr.intern()=="mapred.job.queue.name".intern()) {
                    queue=value;
                }
                jobConfRecord.add("job_conf." + attr, value);
            }
        }
        record.add("JOBCONF-JSON", json.toString());
        record.add("mapred.job.queue.name", queue);
        record.add("JOBID", "job_" + jobID);
        buildGenericRecord(record, null, time, jobData);
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        key.setKey("" + calendar.getTimeInMillis() + "/job_" + jobID + "/" + time);
        output.collect(key, record);

        jobConfRecord.add("JOBID", "job_" + jobID);
        buildGenericRecord(jobConfRecord, null, time, jobConfData);
        output.collect(key, jobConfRecord);
            
        if(!tmp.delete()) {
          log.warn(tmp.getAbsolutePath() + " cannot be deleted.");
        }
      } catch(IOException e) {
        if(out != null) {
          out.close();
        }
        e.printStackTrace();  
        throw e;
      }
  }
  
  public String getDataType() {
    return JobConfProcessor.class.getName();
  }
}
