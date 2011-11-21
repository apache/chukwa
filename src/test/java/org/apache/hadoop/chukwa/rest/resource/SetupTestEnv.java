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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.apache.hadoop.chukwa.hicc.HiccWebServer;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.spi.container.servlet.ServletContainer;

import junit.framework.TestCase;

public class SetupTestEnv extends TestCase {
  public WebResource resource;
  public URI baseURL;
  public Client client;
  public static int restPort = 4080;
  public static Configuration conf = null;
  public static HiccWebServer hicc = null;
  public static String user = "admin";
  public static String authorization = "Basic YWRtaW46YWRtaW4=";
  public static MiniDFSCluster dfs;
  
  public SetupTestEnv() {
    if(hicc==null) {
      hicc = HiccWebServer.getInstance();
      conf = HiccWebServer.getConfig();
    }
  }
  
  public void setUp() {
    hicc.start();
  }
  
  public void tearDown() {
  }
}
