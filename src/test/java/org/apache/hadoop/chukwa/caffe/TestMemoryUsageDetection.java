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
package org.apache.hadoop.chukwa.caffe;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;

import junit.framework.TestCase;

import org.apache.hadoop.chukwa.util.ExceptionUtil;

/**
 * (1) Run non-stop terasort and teragen  
 * (2) Collect memory usage metrics from hbase every 5 minutes for 10 hours and write to csv files in /caffe-test/train/data
 * (3) Create images of dimension 1000 * 200 from /caffe-test/train/data/*.csv. 
 *     The files are saved in /caffe-test/train/data/*png
 * (4) Train the image using caffe
 *
 */

public class TestMemoryUsageDetection extends TestCase {

  /**
   * Run non-stop terasort and teragen to force memory leak
   */
  public void setUp() {
    new Thread(new Runnable() {
      public void run(){
        try {
          String target = new String("/caffe-test/tera/tera.sh");
          Runtime rt = Runtime.getRuntime();
          Process proc = rt.exec(target);
          proc.waitFor();
          BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
          String line = "";                       
          while ((line = reader.readLine())!= null) {
            System.out.println(line + "\n");
          }
        } catch (Exception e) {
          fail(ExceptionUtil.getStackTrace(e));
        }
      }
    }).start();
  }

  public void tearDown() {
  }

  /**
   * Collect memory usage data every 15 min.
   * Stop the timer after 10 hours
   */
  public void testCollectNodeManagerMetrics() {
    int intervalInMin = 15;
    long timerElapseTime = 10 * 60 * 60 * 1000;
    String hostname = "";
    try {
      hostname = InetAddress.getLocalHost().getHostName();
      System.out.println (hostname);
    } catch (IOException e) {
      fail(ExceptionUtil.getStackTrace(e));
    }
    MetricsCollector collector = new MetricsCollector (intervalInMin, hostname);
    collector.start ();
    try {
      Thread.sleep (timerElapseTime);
    } catch (InterruptedException e) {
    }
    collector.cancel ();
      
    // draw images of size 1000 * 200 from the collected csv files
    try {
      ImageCreator generator = new ImageCreator ("/caffe-test/train/data");
      generator.drawImages ();
    } catch (Exception e) {
      fail(ExceptionUtil.getStackTrace(e));
    }
  }

  /**
   * Train the images
   */
  public void testCaffeTrain () {
    try {
      String target = new String("/caffe-test/train/train.sh");
      Runtime rt = Runtime.getRuntime();
      Process proc = rt.exec(target);
      proc.waitFor();
      BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
      String line = "";                       
      while ((line = reader.readLine())!= null) {
        System.out.println(line + "\n");
      }
    } catch (Exception e) {
      fail(ExceptionUtil.getStackTrace(e));
    }    
  }
}
