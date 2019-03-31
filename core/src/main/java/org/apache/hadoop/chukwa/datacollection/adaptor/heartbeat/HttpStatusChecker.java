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
package org.apache.hadoop.chukwa.datacollection.adaptor.heartbeat;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

/**
 * Check the status through http interface. Takes the component name to be included ion
 * the status and the uri as the arguments.
 *
 */
public class HttpStatusChecker implements StatusChecker {
  private String componentName, uri;
  private JSONObject status = new JSONObject();
  Logger log = Logger.getLogger(HttpStatusChecker.class);

  @SuppressWarnings("unchecked")
  @Override
  public void init(String... args) throws StatusCheckerException {
    if(args.length != 2){
      throw new StatusCheckerException("Insufficient number of arguments for HttpStatusChecker");
    }
    componentName = args[0];
    uri = args[1];
    status.put("component", componentName);
    status.put("uri", uri);
  }

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject getStatus() {
    HttpURLConnection connection = null;
    try{
      URL url = new URL(uri);
      connection = (HttpURLConnection)url.openConnection();
      connection.connect();
      status.put("status", "running");
    } catch (IOException e) {
      status.put("status", "stopped");    
    } finally {
      if(connection != null){
        connection.disconnect();
      }
    }
    return status;    
  }
}
