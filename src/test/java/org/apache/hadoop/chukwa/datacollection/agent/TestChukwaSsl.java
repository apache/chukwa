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

package org.apache.hadoop.chukwa.datacollection.agent;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.ChunkQueue;
import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.chukwa.datacollection.agent.ChukwaConstants.*;
import junit.framework.TestCase;

public class TestChukwaSsl extends TestCase{
  String keyStoreFile = "../../test-classes/chukwa.store";
  @Override
  protected void setUp() throws IOException, InterruptedException{
    String[] cmd = new String[]{System.getenv("JAVA_HOME")+"/bin/keytool", "-genkeypair", "-keyalg", "RSA",
        "-alias", "monitoring", "-validity", "36500", "-keystore", keyStoreFile, "-keysize", "1024",
        "-keypass", "chukwa", "-storepass", "chukwa", "-dname", "cn=*,ou=chukwa,o=apache,c=US", "-storetype", "jks"
    };
    Process p = Runtime.getRuntime().exec(cmd);
    p.waitFor();
    if(p.exitValue() != 0){
      BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line;
      while((line = reader.readLine()) != null){
        System.out.println("Output:"+line);
      }
      reader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
      while((line = reader.readLine()) != null){
        System.out.println("Error:"+line);
      }
    }
    System.out.println("keytool exit value:" + p.exitValue());
  }
  
  @Override
  protected void tearDown(){
    new File(keyStoreFile).delete();
  }
    

  public void testRestServer() throws Exception{
    //keystore generated using the following command
    //keytool -genkeypair -keyalg RSA -alias monitoring -validity 36500 -keystore src/test/resources/chukwa.store -keysize 1024 -keypass chukwa -storepass chukwa -dname "cn=*, ou=chukwa, o=apache, c=US" -storetype jks
    Configuration conf = new Configuration();
    conf.set(SSL_ENABLE, "true");
    String keystore = new File(ClassLoader.getSystemResource("chukwa.store").getFile()).getAbsolutePath();
    System.out.println("keystore = "+keystore);
    String commonPassword = "chukwa";
    
    conf.set(KEYSTORE_STORE, keystore);
    conf.set(KEYSTORE_PASSWORD, commonPassword);
    conf.set(KEYSTORE_KEY_PASSWORD, commonPassword);
    conf.set(TRUSTSTORE_STORE, keystore);    
    conf.set(TRUST_PASSWORD, commonPassword);
    /*
    //optional properties     
    String storeType = "pkcs12";
    String sslProtocol = "TLS";   
    conf.set(KEYSTORE_TYPE, storeType);
    conf.set(TRUSTSTORE_TYPE, storeType);
    conf.set(SSL_PROTOCOL, sslProtocol);
    */
    //start agent, which starts chukwa rest server
    ChukwaAgent agent = new ChukwaAgent(conf);
    System.out.println("Started ChukwaRestServer");
    testSecureRestAdaptor(agent);
    agent.shutdown();
    System.out.println("Stopped ChukwaRestServer");
  }
  
  private void testSecureRestAdaptor(ChukwaAgent agent) {
    //add rest adaptor to collect the agent adaptor info through https
    agent.processAddCommand("add RestAdaptor DebugProcessor https://localhost:9090/rest/v2/adaptor 5 0");
    assertEquals(1, agent.adaptorCount());
    final ChunkQueue eventQueue = DataFactory.getInstance().getEventQueue();
    final List<Chunk> chunks = new ArrayList<Chunk>();
    Thread collector = new Thread(){
      @Override
      public void run(){
        try {
          eventQueue.collect(chunks, 1);
        } catch (InterruptedException e) {
        }
      }
    };
    
    //wait 10s and interrupt the collector
    collector.start();
    try {
      collector.join(10000);
    } catch (InterruptedException e) {
    }
    collector.interrupt();    
    
    //make sure we collected atleast 1 chunk
    assertTrue(chunks.size() > 0);
    for(Chunk chunk: chunks){
      String data = new String(chunk.getData());
      System.out.println("Collected chunk - " + data);
    }
  }
}
