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

import java.util.List;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter;
import org.apache.hadoop.chukwa.datacollection.writer.PipelineableWriter;
import org.apache.hadoop.chukwa.datacollection.writer.WriterException;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class SolrWriter extends PipelineableWriter {
  private static Logger log = Logger.getLogger(SolrWriter.class);
  private static CloudSolrServer server;
  private static String ID = "id";
  private static String SEQ_ID = "seqId";
  private static String DATA_TYPE = "type";
  private static String STREAM_NAME = "stream";
  private static String TAGS = "tags";
  private static String SOURCE = "source";
  private static String DATA = "data";

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
    server = new CloudSolrServer(serverName);
    server.setDefaultCollection(collection);
  }

  @Override
  public void close() throws WriterException {
  }

  @Override
  public CommitStatus add(List<Chunk> chunks) throws WriterException {
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
        doc.addField(DATA, new String(chunk.getData()));
        server.add(doc);
        server.commit();
      } catch (Exception e) {
        log.error(ExceptionUtil.getStackTrace(e));
        throw new WriterException("Failed to store data to Solr Cloud.");
      }
    }
    if (next != null) {
      rv = next.add(chunks); //pass data through
    }
    return rv;
  }
}
