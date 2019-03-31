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

import java.util.ArrayList;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.log4j.Logger;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.core.CoreContainer;

import junit.framework.Assert;

public class TestSolrWriter extends SolrJettyTestBase {
  private static Logger log = Logger.getLogger(TestSolrWriter.class);
  private static EmbeddedSolrServer server;
  CoreContainer container;
  
  public void setUp() {
    try {
      String dataDir = System.getProperty("CHUKWA_DATA_DIR", "target/test/var");
      container = new CoreContainer(dataDir);
      container.load();

      server = new EmbeddedSolrServer(container, "collection1" );
      super.setUp();
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      Assert.fail(e.getMessage());
    }    
  }
  
  public void tearDown() throws Exception {
    if (server != null) {
      server.shutdown();
    }
    super.tearDown();
  }
  
  /**
   * Test adding a chunk to solr cloud, then query for the same chunk.
   */
  public void testCommit() {
    ArrayList<Chunk> chunks = new ArrayList<Chunk>();
    chunks.add(new ChunkImpl("Hadoop", "namenode", 
        System.currentTimeMillis(), "This is a test.".getBytes(), null));      
    try {
      QueryResponse rsp = server.query(new SolrQuery("*:*"));
      Assert.assertEquals(0, rsp.getResults().getNumFound());

      SolrWriter sw = new SolrWriter();
      sw.add(chunks);
      
      // TODO: not a great way to test this - timing is easily out
      // of whack due to parallel tests and various computer specs/load
      Thread.sleep(1000); // wait 1 sec
      
      // now check that it comes out...
      rsp = server.query(new SolrQuery("data:test"));
      
      int cnt = 0;
      while (rsp.getResults().getNumFound() == 0) {
        // wait and try again for slower/busier machines
        // and/or parallel test effects.
        
        if (cnt++ == 10) {
          break;
        }
        
        Thread.sleep(2000); // wait 2 seconds...
        
        rsp = server.query(new SolrQuery("data:test"));
      }
      
      Assert.assertEquals(1, rsp.getResults().getNumFound());
      
    } catch (Exception e) {
      
    }
  }
}
