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
package org.apache.hadoop.chukwa.rest.resource;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.writer.PipelineStageWriter;
import org.apache.hadoop.chukwa.datacollection.writer.SocketTeeWriter;
import org.apache.hadoop.chukwa.datacollection.writer.WriterException;
import org.apache.hadoop.chukwa.rest.bean.ClientTraceBean;
import org.apache.hadoop.chukwa.rest.bean.UserBean;
import org.apache.hadoop.chukwa.rest.bean.WidgetBean;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.GenericType;

public class TestClientTrace extends SetupTestEnv {  
  public void testClientTrace() {
    // Setup Collector
    Configuration conf = new Configuration();  
    conf.set("chukwaCollector.pipeline",
        SocketTeeWriter.class.getCanonicalName());
    conf.set("chukwaCollector.writerClass", 
        PipelineStageWriter.class.getCanonicalName());    
    PipelineStageWriter psw = new PipelineStageWriter();
    try {
      psw.init(conf);
      // Send a client trace chunk
      ArrayList<Chunk> l = new ArrayList<Chunk>();
      String line = "2009-12-29 22:32:27,047 INFO org.apache.hadoop.hdfs.server.datanode.DataNode.clienttrace: src: /10.10.100.60:43707, dest: /10.10.100.60:50010, bytes: 7003141, op: HDFS_WRITE, cliID: DFSClient_-8389654, offset: 0, srvID: DS-2032680158-98.137.100.60-50010-1259976007324, blockid: blk_-2723720761101769540_705411, duration: 289013780000";      
      l.add(new ChunkImpl("ClientTrace", "name", 1, line.getBytes(), null));
      assertTrue(l.size()==1);
      psw.add(l);
      assertTrue(true);
    } catch (WriterException e) {
      fail(ExceptionUtil.getStackTrace(e));
    }
    
    try {
      // Locate the client trace object
      client = Client.create();
      resource = client.resource("http://localhost:"+restPort);
      List<ClientTraceBean> list = resource.path("/hicc/v1/clienttrace").header("Authorization", authorization).get(new GenericType<List<ClientTraceBean>>(){});
      for(ClientTraceBean ctb : list) {
        assertEquals("HDFS_WRITE", ctb.getAction());
      }
    } catch (Exception e) {
      fail(ExceptionUtil.getStackTrace(e));
    }    
  }
}
