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

import java.util.Calendar;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.log4j.Logger;
import org.apache.hadoop.chukwa.util.ExceptionUtil;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

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
        byte[] data = bean.getBytes();
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
    c = Client.create();
    return s;
  }

}
