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
package org.apache.hadoop.chukwa.datatrigger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.HashMap;

/**
 * Trigger action that makes an HTTP request when executed.
 * <P>
 * To use this trigger, two types of configurations must be set. First, this class
 * must be configured to be invoked for a given trigger event. Second, the
 * the relevant settings for the HTTP request(s) to be made must be set as
 * described below.
 * <P>
 * The general format of this classes configs is
 * <code>chukwa.trigger.[eventName].http.[N].[paramName]</code> where
 * <code>eventName</code> is the name of the event the request values are bound
 * to (see TriggerEvent), <code>N</code> is a counter for each request configured (starting at 1)
 * and <code>paramName</code> is the request parameter being set.
 * <P>
 * Using the post demux success trigger event as an example, the first request
 * to be fired would use the following configurations
 * <ul>
 * <li><code>chukwa.trigger.post.demux.success.http.1.url</code> - The HTTP url to
 * invoke.</li>
 * <li><code>chukwa.trigger.post.demux.success.http.1.method</code> - The HTTP method
 * (optional, default=GET).</li>
 * <li><code>chukwa.trigger.post.demux.success.http.1.headers</code> - A comma-delimited
 * set of HTTP headers (in <code>[headerName]:[headerValue]</code> form) to
 * include (optional).</li>
 * <li><code>chukwa.trigger.post.demux.success.http.1.body</code> - The text HTTP body
 * to include (optional).</li>
 * <li><code>chukwa.trigger.post.demux.success.http.1.connect.timeout</code> - The
 * HTTP connection timeout setting in milliseconds (optional, default=5000ms).</li>
 * <li><code>chukwa.trigger.post.demux.success.http.1.read.timeout</code> - The
 * HTTP read timeout setting in milliseconds (optional, default=5000ms).</li>
 * </ul>
 * @see TriggerAction
 * @see TriggerEvent
 */
public class HttpTriggerAction implements TriggerAction {
  protected Log log = LogFactory.getLog(getClass());


  /**
   * Iterates over each URL found, fetched other settings and fires and HTTP
   * request.
   *
   * @param conf
   * @param fs
   * @param src
   * @param triggerEvent
   * @throws IOException
   */
  public void execute(Configuration conf, FileSystem fs,
                      FileStatus[] src, TriggerEvent triggerEvent) throws IOException {

    if (log.isDebugEnabled()) {
      for (FileStatus file : src) {
          log.debug("Execute file: " + file.getPath());
      }
    }

    int reqNumber = 1;
    URL url = null;
    while ((url = getUrl(conf, triggerEvent, reqNumber)) != null) {

      // get settings for this request
      String method = getMethod(conf, triggerEvent, reqNumber);
      Map<String, String> headers = getHeaders(conf, triggerEvent, reqNumber);
      String body = getBody(conf, triggerEvent, reqNumber);
      int connectTimeout = getConnectTimeout(conf, triggerEvent, reqNumber);
      int readTimeout = getReadTimeout(conf, triggerEvent, reqNumber);

      try {
        // make the request
        makeHttpRequest(url, method, headers, body, connectTimeout, readTimeout);
      }
      catch(Exception e) {
        log.error("Error making request to " + url, e);
      }
      reqNumber++;
    }
  }

  private void makeHttpRequest(URL url, String method,
                               Map<String, String> headers, String body,
                               int connectTimeout, int readTimeout) throws IOException {
    if (url == null) {
      return;
    }

    // initialize the connection
    HttpURLConnection conn = (HttpURLConnection)url.openConnection();
    conn.setRequestMethod(method);
    conn.setDoInput(true);
    conn.setConnectTimeout(connectTimeout);
    conn.setReadTimeout(readTimeout);

    // set headers
    boolean contentLengthExists = false;
    if (headers != null) {
      for (String name: headers.keySet()) {
        if (log.isDebugEnabled()) {
          log.debug("Setting header " + name + ": " + headers.get(name));
        }
        if (name.equalsIgnoreCase("content-length")) {
          contentLengthExists = true;
        }
        conn.setRequestProperty(name, headers.get(name));
      }
    }

    // set content-length if not already set
    if (!"GET".equals(method) && !contentLengthExists) {
      String contentLength = body != null ? String.valueOf(body.length()) : "0";
      conn.setRequestProperty("Content-Length", contentLength);
    }

    // send body if it exists
    if (body != null) {
      conn.setDoOutput(true);
      OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());
      writer.write(body);
      writer.flush();
      writer.close();
    }
    else {
      conn.setDoOutput(false);
    }

    // read reponse code/message and dump response
    log.info("Making HTTP " + method + " to: " + url);
    int responseCode = conn.getResponseCode();
    log.info("HTTP Response code: " + responseCode);


    if (responseCode != 200) {
      log.info("HTTP Response message: " + conn.getResponseMessage());
    }
    else {
      BufferedReader reader = new BufferedReader(
                                new InputStreamReader(conn.getInputStream()));
      String line;
      StringBuilder sb = new StringBuilder();
      while ((line = reader.readLine()) != null) {
        if(sb.length() > 0) {
          sb.append("\n");
        }
        sb.append(line);
      }
      log.info("HTTP Response:\n" + sb);

      reader.close();
    }

    conn.disconnect();
  }

  protected URL getUrl(Configuration conf,
                       TriggerEvent triggerEvent,
                       int reqNumber) throws MalformedURLException {
    String urlString = conf.get(getConfigKey(triggerEvent, reqNumber, "url"), null);
    if (urlString == null) {
      return null;
    }

    return new URL(urlString);
  }

  protected String getMethod(Configuration conf,
                             TriggerEvent triggerEvent,
                             int reqNumber) {
    return conf.get(getConfigKey(triggerEvent, reqNumber, "method"), "GET");
  }

  protected Map<String, String> getHeaders(Configuration conf,
                                           TriggerEvent triggerEvent,
                                           int reqNumber) {
    Map<String, String> headerMap = new HashMap<String,String>();

    String headers = conf.get(getConfigKey(triggerEvent, reqNumber, "headers"), null);

    if (headers != null) {
      String[] headersSplit = headers.split(",");
      for (String header : headersSplit) {
        String[] nvp = header.split(":", 2);
        if (nvp.length < 2) {
          log.error("Invalid HTTP header found: " + nvp);
          continue;
        }
        headerMap.put(nvp[0].trim(), nvp[1].trim());
      }
    }

    return headerMap;
  }

  protected String getBody(Configuration conf,
                           TriggerEvent triggerEvent,
                           int reqNumber) {
    return conf.get(getConfigKey(triggerEvent, reqNumber, "body"), "GET");
  }

  protected int getConnectTimeout(Configuration conf,
                                 TriggerEvent triggerEvent,
                                 int reqNumber) {
    String timeout = conf.get(getConfigKey(triggerEvent, reqNumber, "connect.timeout"), null);
    return timeout != null ? Integer.parseInt(timeout) : 5000;
  }

  
  protected int getReadTimeout(Configuration conf,
                              TriggerEvent triggerEvent,
                              int reqNumber) {
    String timeout = conf.get(getConfigKey(triggerEvent, reqNumber, "read.timeout"), null);
    return timeout != null ? Integer.parseInt(timeout) : 5000;
  }

  private String getConfigKey(TriggerEvent triggerEvent, int reqNumber, String name) {
    return triggerEvent.getConfigKeyBase() + ".http." + reqNumber + "." + name;
  }
}
