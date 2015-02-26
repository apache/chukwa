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
package org.apache.hadoop.chukwa.datacollection.adaptor;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.PipelineConnector;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.json.simple.parser.JSONParser;

import junit.framework.TestCase;

public class TestHeartbeatAdaptor extends TestCase {
  private volatile boolean shutdown = false;
  private final int port = 4321;
  public void testPingAdaptor() throws IOException, InterruptedException{
    ChukwaAgent agent = ChukwaAgent.getAgent();
    Configuration conf = agent.getConfiguration();
    conf.set("chukwa.http.writer.host", "localhost");
    conf.set("chukwa.http.writer.port", String.valueOf(port));
    conf.set("chukwa.pipeline", "org.apache.hadoop.chukwa.datacollection.writer.HttpWriter");
    agent.connector = new PipelineConnector();
    agent.connector.start();
    System.out.println("Started connector");
    
    String adaptor = agent.processAddCommand("add HeartbeatAdaptor DefaultProcessor (ChukwaStatusChecker, HttpStatusChecker Invalid.component http://localhost:4322, HttpStatusChecker Chukwa.rest.server http://localhost:9090/rest/v2) 3 0");
    //assertTrue(agent.adaptorCount() == 1);
    if(agent.connector != null){
      agent.connector.shutdown();
    }
    
    LocalServer server = new LocalServer();
    server.start();
    
    try {
      server.join(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    if(server.getFailMessage() != null){
      fail(server.getFailMessage());
    }
    assertTrue(server.messageCount > 0);
    server.interrupt();
    agent.stopAdaptor(adaptor, false);
    agent.shutdown();    
  }
  
  class LocalServer extends Thread {
    ServerSocket sock;
    String failMessage = null;
    int messageCount = 0;
    
    LocalServer() throws IOException{
      sock = new ServerSocket();
      sock.setReuseAddress(true);
      sock.bind(new InetSocketAddress(port));
      System.out.println("Started local server");      
    }
    
    //calling fail() from this thread will not cause testcase to fail. So propagate error to main thread.
    String getFailMessage(){
      return failMessage;
    }
    
    int getMessageCount(){
      return messageCount;
    }
    
    @Override
    public void run(){
      while(!shutdown){
        try {
          Socket socket = sock.accept();
          DataInputStream dis = new DataInputStream(socket.getInputStream());
          int size;
          try{
            while((size = dis.readInt()) > 0){
              if(size > 1024){
                fail();
              }
              messageCount++;
              byte[] buffer = new byte[size];
              dis.read(buffer);
              String data = new String(buffer);
              System.out.println("Received:"+data);
              JSONParser json  = new JSONParser();
              //make sure we have a parseable json
              json.parse(data);
            }
          } catch(java.io.EOFException e){
            System.out.println("reached end of stream, so closing this socket");
          } finally {
            socket.close();            
          }
        } catch (Exception e) {
          failMessage = ExceptionUtil.getStackTrace(e);
        }
      }
    }
  }
}
