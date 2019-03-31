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

package org.apache.hadoop.chukwa.util;

import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.WebResource;

/* This should contain set of helper methods to convert the 
 * response returned from any web servers to required formats
 * This can be modified to accept different headers based on the 
 * file formats.
 */
public class RestUtil {

  private static WebResource webResource;
  private static Client webClient;
  private static Logger log = Logger.getLogger(RestUtil.class);

  public static String getResponseAsString(String URI) {
    if (URI == null) {
      throw new IllegalStateException("URI cannot be blank");
    }

    String response = null;
    webClient = Client.create();
    try {
      webResource = webClient.resource(URI);
      response = webResource.accept(MediaType.APPLICATION_JSON_TYPE).get(
          String.class);
    } catch (ClientHandlerException e) {
      Throwable t = e.getCause();
      if (t instanceof java.net.ConnectException) {
        log.warn("Connect exception trying to connect to [" + URI
            + "]. Make sure the service is running");
      } else {
        log.error(ExceptionUtil.getStackTrace(e));
      }
    } finally {
      if (webClient != null) {
        webClient.destroy();
      }
    }
    return response;
  }

}