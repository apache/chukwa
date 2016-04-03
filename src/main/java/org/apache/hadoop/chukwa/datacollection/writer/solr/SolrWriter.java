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
package org.apache.hadoop.chukwa.datacollection.writer.solr;

import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter;
import org.apache.hadoop.chukwa.datacollection.writer.PipelineableWriter;
import org.apache.hadoop.chukwa.datacollection.writer.WriterException;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;

public class SolrWriter extends PipelineableWriter {
  private static Logger log = Logger.getLogger(SolrWriter.class);
  private CloudSolrClient client;
  private final static String ID = "id";
  private final static String SEQ_ID = "seqId";
  private final static String DATA_TYPE = "type";
  private final static String STREAM_NAME = "stream";
  private final static String TAGS = "tags";
  private final static String SOURCE = "source";
  private final static String DATA = "data";
  private final static String USER = "user";
  private final static String SERVICE = "service";
  private final static String DATE = "date";
  private final static Pattern userPattern = Pattern.compile("user=(.+?)[, ]");
  private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

  public SolrWriter() throws WriterException {
    init(ChukwaAgent.getStaticConfiguration());
  }
  
  @Override
  public void init(Configuration c) throws WriterException {
    String serverName = c.get("solr.cloud.address");
    if (serverName == null) {
      throw new WriterException("Solr server address is not defined.");
    }
    String collection = c.get("solr.collection", "logs");
    if(client == null) {
      client = new CloudSolrClient(serverName);
      client.setDefaultCollection(collection);
    }
  }

  @Override
  public void close() throws WriterException {
  }

  @Override
  public CommitStatus add(List<Chunk> chunks) throws WriterException {
    if(client == null) {
      init(ChukwaAgent.getStaticConfiguration());
    }
    CommitStatus rv = ChukwaWriter.COMMIT_OK;
    for(Chunk chunk : chunks) {
      try {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField(ID, chunk.getSource() + "_" + chunk.getSeqID());
        doc.addField(TAGS, chunk.getTags());
        doc.addField(STREAM_NAME, chunk.getStreamName());
        doc.addField(SOURCE, chunk.getSource());
        doc.addField(SEQ_ID, chunk.getSeqID());
        doc.addField(DATA_TYPE, chunk.getDataType());
        doc.addField(DATA, new String(chunk.getData(), Charset.forName("UTF-8")));
        
        // TODO: improve parsing logic for more sophisticated tagging
        String data = new String(chunk.getData(), Charset.forName("UTF-8"));
        Matcher m = userPattern.matcher(data);
        if(m.find()) {
          doc.addField(USER, m.group(1));
        } else {
          doc.addField(USER, "Unclassified");
        }
        if(data.contains("hdfs")) {
          doc.addField(SERVICE, "hdfs");
        } else if(data.contains("yarn")) {
          doc.addField(SERVICE, "yarn");
        } else if(data.contains("mapredice")) {
          doc.addField(SERVICE, "mapreduce");
        } else  if(data.contains("hbase")) {
          doc.addField(SERVICE, "hbase");
        } else {
          doc.addField(SERVICE, "Unclassified");
        }
        try {
          Date d = sdf.parse(data);
          doc.addField(DATE, d, 1.0f);
        } catch(ParseException e) {
          
        }
        client.add(doc);
      } catch (Exception e) {
        log.warn("Failed to store data to Solr Cloud.");
        log.warn(ExceptionUtil.getStackTrace(e));
        client = null;
      }
    }
    try {
      if(client != null) {
        client.commit();
      }
    } catch (Exception e) {
      log.warn("Failed to store data to Solr Cloud.");
      log.warn(ExceptionUtil.getStackTrace(e));
    }
    if (next != null) {
      rv = next.add(chunks); //pass data through
    }
    return rv;
  }
}
