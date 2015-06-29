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

import java.io.FileInputStream;
import java.nio.charset.Charset;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.log4j.Logger;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.chukwa.datacollection.agent.ChukwaConstants.*;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HTTPSProperties;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.ws.rs.core.MediaType;

public class RestAdaptor extends AbstractAdaptor {

  private String uri;
  private long period = 60;
  private static Logger log = Logger.getLogger(RestAdaptor.class);
  private WebResource resource;
  private Client c;
  private String bean;
  private Timer timer;
  private TimerTask runner;
  private long sendOffset;

  class RestTimer extends TimerTask {

    private ChunkReceiver receiver;
    private RestAdaptor adaptor;

    RestTimer(ChunkReceiver receiver, RestAdaptor adaptor) {
      this.receiver = receiver;
      this.adaptor = adaptor;
    }

    @Override
    public void run() {
      try {
        resource = c.resource(uri);
        bean = resource.accept(MediaType.APPLICATION_JSON_TYPE).get(
            String.class);
        byte[] data = bean.getBytes(Charset.forName("UTF-8"));
        sendOffset += data.length;
        ChunkImpl c = new ChunkImpl(type, "REST", sendOffset, data, adaptor);
        long rightNow = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
            .getTimeInMillis();
        c.addTag("timeStamp=\"" + rightNow + "\"");
        receiver.add(c);
      } catch (com.sun.jersey.api.client.ClientHandlerException e) {
        Throwable t = e.getCause();
        if (t instanceof java.net.ConnectException) {
          log.warn("Connect exception trying to connect to " + uri
              + ". Make sure the service is running");
        } else {
          log.error("RestAdaptor: Interrupted exception");
          log.error(ExceptionUtil.getStackTrace(e));
        }
      } catch (Exception e) {
        log.error("RestAdaptor: Interrupted exception");
        log.error(ExceptionUtil.getStackTrace(e));
      }
    }
  }

  @Override
  public String getCurrentStatus() {
    StringBuilder buffer = new StringBuilder();
    buffer.append(type);
    buffer.append(" ");
    buffer.append(uri);
    buffer.append(" ");
    buffer.append(period);
    return buffer.toString();
  }

  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy)
      throws AdaptorException {
    timer.cancel();
    return sendOffset;
  }

  @Override
  public void start(long offset) throws AdaptorException {
    sendOffset = offset;
    if (timer == null) {
      timer = new Timer();
      runner = new RestTimer(dest, RestAdaptor.this);
    }
    timer.scheduleAtFixedRate(runner, 0, period * 1000);
  }

  @Override
  public String parseArgs(String s) {
    // RestAdaptor [Host] port uri [interval]
    String[] tokens = s.split(" ");
    if (tokens.length == 2) {
      uri = tokens[0];
      try {
        period = Integer.parseInt(tokens[1]);
      } catch (NumberFormatException e) {
        log.warn("RestAdaptor: incorrect argument for period. Expecting number");
        return null;
      }
    } else {
      log.warn("bad syntax in RestAdaptor args");
      return null;
    }
    try {
      initClient();
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      return null;
    }    
    return s;
  }
  
  private void initClient() throws Exception {
    if (uri.contains("https")) {
      Configuration conf = ChukwaAgent.getAgent().getConfiguration();
      String trustStoreFile = conf.get(TRUSTSTORE_STORE);
      String trustStorePw = conf.get(TRUST_PASSWORD);
      if (trustStoreFile == null || trustStorePw == null) {
        throw new Exception(
            "Cannot instantiate RestAdaptor to uri "
                + uri
                + " due to missing trust store configurations chukwa.ssl.truststore.store and chukwa.ssl.trust.password");
      }
      String trustStoreType = conf.get(TRUSTSTORE_TYPE, DEFAULT_STORE_TYPE);
      KeyStore trustStore = KeyStore.getInstance(trustStoreType);
      FileInputStream fis = null;
      try {
        fis = new FileInputStream(trustStoreFile);
        trustStore.load(fis, trustStorePw.toCharArray());
      } finally {
        if (fis != null) {
          fis.close();
        }
      }
      TrustManagerFactory tmf = TrustManagerFactory
          .getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(trustStore);
      TrustManager[] trustManagers = tmf.getTrustManagers();

      SSLContext ctx = null;
      String protocol = conf.get(SSL_PROTOCOL, DEFAULT_SSL_PROTOCOL);
      ctx = SSLContext.getInstance(protocol);
      ctx.init(null, trustManagers, new SecureRandom());
      ClientConfig cc = new DefaultClientConfig();
      HTTPSProperties props = new HTTPSProperties(null, ctx);
      cc.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, props);
      c = Client.create(cc);
    } else {
      c = Client.create();
    }
  }

}
