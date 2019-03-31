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

package org.apache.hadoop.chukwa.datacollection.test;


import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.ServletCollector;
import org.apache.hadoop.chukwa.datacollection.connector.http.HttpConnector;
import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;
import org.apache.hadoop.chukwa.datacollection.writer.ConsoleWriter;
import org.apache.hadoop.chukwa.datacollection.writer.SeqFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import java.io.*;
import java.util.*;

public class FileTailerStressTest {

  static final int DELAY_MIN = 10 * 1000;
  static final int DELAY_RANGE = 2 * 1000;
  static final Logger log = Logger.getLogger(FileTailerStressTest.class);

  static class OccasionalWriterThread extends Thread {
    File file;

    OccasionalWriterThread(File f) {
      file = f;
    }

    public void run() {
      PrintWriter out = null;
      try {
        out = new PrintWriter(file.getAbsolutePath(), "UTF-8");
        Random rand = new Random();
        while (true) {
          int delay = rand.nextInt(DELAY_RANGE) + DELAY_MIN;
          Thread.sleep(delay);
          Date d = new Date();
          out.println("some test data written at " + d.toString());
          out.flush();
        }
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        if(out != null) {
          out.close();
        }
      }
    }
  }

  static int FILES_TO_USE = 100;

  /**
   * @param args is command line parameters
   */
  public static void main(String[] args) {
    try {
      Server server = new Server(9990);
      Context root = new Context(server, "/", Context.SESSIONS);

      Configuration conf =  new Configuration();
      ServletCollector collector = new ServletCollector(conf);
      collector.setWriter(new ConsoleWriter(true));
      root.addServlet(new ServletHolder(collector), "/*");
      server.start();
      server.setStopAtShutdown(false);

      Thread.sleep(1000);
      ChukwaAgent agent = ChukwaAgent.getAgent();
      HttpConnector connector = new HttpConnector(agent,
          "http://localhost:9990/chukwa");
      connector.start();

      ChukwaConfiguration cc = new ChukwaConfiguration();
      int portno = cc.getInt("chukwaAgent.control.port", 9093);
      ChukwaAgentController cli = new ChukwaAgentController("localhost", portno);

      File workdir = new File("/tmp/stresstest/");
      if(!workdir.mkdir()) {
        log.warn("Error creating working directory:" + workdir.getAbsolutePath());
      }
      for (int i = 0; i < FILES_TO_USE; ++i) {
        File newTestF = new File("/tmp/stresstest/" + i);

        newTestF.deleteOnExit();
        (new OccasionalWriterThread(newTestF)).start();
        cli.addFile("test-lines", newTestF.getAbsolutePath());
      }

      Thread.sleep(60 * 1000);
      System.out.println("cleaning up");
      if(!workdir.delete()) {
        log.warn("Error clean up working directory:" + workdir.getAbsolutePath());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
