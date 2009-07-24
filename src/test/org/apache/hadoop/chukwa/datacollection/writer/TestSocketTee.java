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
package org.apache.hadoop.chukwa.datacollection.writer;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.Chunk;
import java.util.ArrayList;
import org.apache.hadoop.chukwa.datacollection.collector.CaptureWriter;
import java.net.*;
import java.io.*;

public class TestSocketTee  extends TestCase{
  public void testSocketTee() throws Exception {
    
    Configuration conf = new Configuration();  
    
    conf.set("chukwaCollector.pipeline",
        SocketTeeWriter.class.getCanonicalName()+","// note comma
        + CaptureWriter.class.getCanonicalName());
    
    conf.set("chukwaCollector.writerClass", 
        PipelineStageWriter.class.getCanonicalName());
    
    PipelineStageWriter psw = new PipelineStageWriter();
    psw.init(conf);

    System.out.println("pipeline established; now pushing a chunk");
    ArrayList<Chunk> l = new ArrayList<Chunk>();
    l.add(new ChunkImpl("dt", "name", 1, new byte[] {'a'}, null));
    psw.add(l);
    //push a chunk through. It should get written, but the socket tee shouldn't do anything.
    assertEquals(1, CaptureWriter.outputs.size());
    //now connect and set up a filter.
    
    System.out.println("connecting to localhost");
    Socket s = new Socket("localhost", SocketTeeWriter.DEFAULT_PORT);
 //   s.setSoTimeout(2000);
    DataOutputStream dos = new DataOutputStream (s.getOutputStream());
    dos.write((SocketTeeWriter.WRITABLE + " datatype=dt3\n").getBytes());
    DataInputStream dis = new DataInputStream(s.getInputStream());

    System.out.println("command send");

    dis.readFully(new byte[3]);
    //push a chunk not matching filter -- nothing should happen.
    l = new ArrayList<Chunk>();
    l.add(new ChunkImpl("dt2", "name", 1, new byte[] {'b'}, null));
    psw.add(l);
    assertEquals(2, CaptureWriter.outputs.size());

    System.out.println("sent nonmatching chunk");

    //and now one that does match -- data should be available to read off the socket

    l = new ArrayList<Chunk>();
    l.add(new ChunkImpl("dt3", "name", 1, new byte[] {'c'}, null));
    psw.add(l);
    assertEquals(3, CaptureWriter.outputs.size());

    System.out.println("sent matching chunk");
    
    System.out.println("reading...");
    ChunkImpl chunk = ChunkImpl.read(dis);
    assertTrue(chunk.getDataType().equals("dt3"));
    System.out.println(chunk);

    dis.close();
    dos.close();
    s.close();
    
    Socket s2 = new Socket("localhost", SocketTeeWriter.DEFAULT_PORT);
    s2.getOutputStream().write((SocketTeeWriter.RAW+" content=.*c.*\n").getBytes());
    dis = new DataInputStream(s2.getInputStream());
    dis.readFully(new byte[3]); //read "OK\n"
    l = new ArrayList<Chunk>();
    l.add(new ChunkImpl("dt3", "name", 1, new byte[] {'c'}, null));
    psw.add(l);
    int len = dis.readInt();
    assertTrue(len == 1);
    byte[] data = new byte[100];
    int read = dis.read(data);
    assertTrue(read == 1);
    assertTrue(data[0] == 'c');
  }
  
}
