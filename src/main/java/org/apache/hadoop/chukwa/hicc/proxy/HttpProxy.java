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
package org.apache.hadoop.chukwa.hicc.proxy;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.log4j.Logger;

/**
 * HTTP Proxy Servlet for Solr
 *
 */
public class HttpProxy extends HttpServlet {
  private static final long serialVersionUID = 7574L;
  private final String USER_AGENT = "Mozilla/5.0";
  private final static String SOLR_URL = "chukwa.solr.url";
  private final static Logger LOG = Logger.getLogger(HttpProxy.class);
  private String solrUrl = null;

  public HttpProxy() {
    super();
    ChukwaConfiguration conf = new ChukwaConfiguration();
    solrUrl = conf.get(SOLR_URL);
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    // Create Get request dynamically to remote server
    String url = solrUrl + request.getPathInfo()
        + "?" + request.getQueryString();

    URL obj = new URL(url);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();

    // optional default is GET
    con.setRequestMethod("GET");

    // add request header
    con.setRequestProperty("User-Agent", USER_AGENT);

    int responseCode = con.getResponseCode();
    LOG.info("Sending 'GET' request to URL : " + url);
    LOG.info("Response Code : " + responseCode);

    BufferedReader in = new BufferedReader(new InputStreamReader(
        con.getInputStream(), Charset.forName("UTF-8")));
    String inputLine;
    StringBuffer response1 = new StringBuffer();

    ServletOutputStream sout = response.getOutputStream();

    while ((inputLine = in.readLine()) != null) {
      response1.append(inputLine);
      sout.write(inputLine.getBytes(Charset.forName("UTF-8")));
    }
    in.close();

    sout.flush();

  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    // Create Post request dynamically to remote server
    String url = solrUrl + request.getPathInfo();

    URL obj = new URL(url);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();

    // add reuqest header
    con.setRequestMethod("POST");
    con.setRequestProperty("User-Agent", USER_AGENT);
    con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");

    StringBuilder sb = new StringBuilder();
    @SuppressWarnings("rawtypes")
    Map map = request.getParameterMap();
    for (Object key : request.getParameterMap().keySet()) {
      if (sb.length() > 0) {
        sb.append('&');
      }
      String keyStr = (String) key;
      String[] temp = (String[]) map.get(keyStr);
      for (String s : temp) {
        sb.append(URLEncoder.encode(keyStr, "UTF-8")).append('=')
            .append(URLEncoder.encode(s, "UTF-8"));
      }
    }

    String urlParameters = sb.toString();

    // Send post request
    con.setDoOutput(true);
    DataOutputStream wr = new DataOutputStream(con.getOutputStream());
    wr.writeBytes(urlParameters);
    wr.flush();
    wr.close();

    int responseCode = con.getResponseCode();
    LOG.debug("\nSending 'POST' request to URL : " + url);
    LOG.debug("Post parameters : " + urlParameters);
    LOG.debug("Response Code : " + responseCode);

    BufferedReader in = new BufferedReader(new InputStreamReader(
        con.getInputStream(), Charset.forName("UTF-8")));
    String inputLine;
    StringBuffer response1 = new StringBuffer();

    ServletOutputStream sout = response.getOutputStream();

    while ((inputLine = in.readLine()) != null) {
      response1.append(inputLine);
      sout.write(inputLine.getBytes(Charset.forName("UTF-8")));
    }
    in.close();

    sout.flush();
  }

}
