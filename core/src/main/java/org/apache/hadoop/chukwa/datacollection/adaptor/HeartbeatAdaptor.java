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

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.adaptor.heartbeat.StatusChecker;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class HeartbeatAdaptor extends AbstractAdaptor {

  private final Logger log = Logger.getLogger(HeartbeatAdaptor.class);  
  private Timer timer = new Timer();
  JSONObject status = new JSONObject();
  private int period = 3;
  private List<StatusChecker> allCheckers = new ArrayList<StatusChecker>();
  private final String DEFAULT_PACKAGE = "org.apache.hadoop.chukwa.datacollection.adaptor.heartbeat";
  private String arguments;
  long seqId = 0;
  private final String STREAM_NAME = "STATUS";
  private boolean _shouldUseConnector = false;
  private String _host;
  private int _port;

  
  class Task extends TimerTask{
    @Override
    public void run() {
      try {
        heartbeat();
      } catch (InterruptedException e) {
        log.error(ExceptionUtil.getStackTrace(e));
      }      
    }
    @SuppressWarnings("unchecked")
    private void heartbeat() throws InterruptedException {
      status.put("time", System.currentTimeMillis());
      JSONArray array = new JSONArray();
      for (StatusChecker checker : allCheckers) {
        array.add(checker.getStatus());
      }
      status.put("components", array);
      if(_shouldUseConnector){
        ChunkImpl chunk = new ChunkImpl(type, STREAM_NAME, seqId, status.toString()
            .getBytes(Charset.forName("UTF-8")), HeartbeatAdaptor.this);
        dest.add(chunk);
      } else {
        sendDirectly(status.toString());
      }
      seqId++;
    }
    
    private void sendDirectly(String data) {
      DataOutputStream dos = null;
      Socket sock = null;
      byte[] bdata = data.getBytes(Charset.forName("UTF-8"));
      try {
        sock = new Socket(_host, _port);
        dos = new DataOutputStream(sock.getOutputStream());
        dos.writeInt(bdata.length);
        dos.write(bdata);
        dos.flush();
      } catch (Exception e) {
        log.debug(ExceptionUtil.getStackTrace(e));
      } finally {
        if (dos != null) {
          try {
            dos.close();
          } catch (IOException e) {
            log.debug("Error closing dataoutput stream:" + e);
          }
        }
        if (sock != null) {
          try {
            sock.close();
          } catch (IOException e) {
            log.debug("Error closing socket: " + e);
          }
        }
      }
    }

  }
  
  @Override
  public String getCurrentStatus() {
    return type + " " + arguments;
  }

  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy)
      throws AdaptorException {
    timer.cancel();
    return seqId;
  }

  @Override
  public void start(long offset) throws AdaptorException {
    seqId = offset;
    timer.scheduleAtFixedRate(new Task(), 0, period * 1000);    
  }

  @Override
  public String parseArgs(String s) {
    // match patterns like localhost 1234 (aa host1 port1, bb, cc host2 port2) 60
    Pattern p1 = Pattern.compile("(\\(.*\\),?)+\\s(\\d+)");
    Matcher m1 = p1.matcher(s);
    if(!m1.matches()){
      log.error("Invalid adaptor parameters. Usage: HeartbeatAdaptor DefaultProcessor <host> <port> <list-of-status-checkers> <period> <offset>");
      return null;
    }
    try{
      String providers = m1.group(1);
      period = Integer.parseInt(m1.group(2));
      // match pattern like (aa host1 port1, bb, cc host2, port2) and capture the string without braces
      Pattern p2 = Pattern.compile("\\(((?:(?:[\\w/:\\.]+\\s*)+,?\\s*)+)\\)");
      Matcher m2 = p2.matcher(providers);
      if(!m2.matches()){
        log.error("Invalid adaptor parameters. Usage: PingAdaptor DefaultProcessor <host> <port> <list-of-status-providers> <period> <offset>");
        log.error("Specify list of status-providers as (provider1 args1, provider2 args2...). Pattern used for matching:"+p2.pattern());
        return null;
      }
      String[] checkerList = m2.group(1).split(",");
      for(String checker: checkerList){
        String args[] = checker.trim().split(" ");
        String checkerName = args[0];
        try {          
          Object c = Class.forName(checkerName).newInstance();
          if(StatusChecker.class.isInstance(c)){
            StatusChecker sp = (StatusChecker)c;
            sp.init(Arrays.copyOfRange(args, 1, args.length));
            allCheckers.add(sp);
          } else {
            throw new Exception("Unsupported checker:"+checkerName);
          }
        } catch (Exception e) {
          log.debug("Error instantiating StatusChecker:" + checkerName + " due to " + e);
          
          String newProvider = DEFAULT_PACKAGE + "." + checkerName;
          log.debug("Trying with default package name " + DEFAULT_PACKAGE);
          try {
            Object c = Class.forName(newProvider).newInstance();
            if(StatusChecker.class.isInstance(c)){
              StatusChecker sp = (StatusChecker)c;
              sp.init(Arrays.copyOfRange(args, 1, args.length));
              allCheckers.add(sp);
            } else {
              log.error("Unsupported StatusChecker:"+newProvider);
              return null;
            }
          } catch (Exception e1) {
            log.error("Error instantiating StatusChecker:" + checker + " due to " + e1);
            log.error(ExceptionUtil.getStackTrace(e1));
            return null;
          }
        }
      }
    } catch(NumberFormatException nfe){
      log.error(ExceptionUtil.getStackTrace(nfe));
      return null;
    }
    arguments = s;
    Configuration chukwaConf = ChukwaAgent.getStaticConfiguration();
    _host = chukwaConf.get("chukwa.http.writer.host", "localhost");
    _port = Integer.parseInt(chukwaConf.get("chukwa.http.writer.port", "8802"));
    String connector = chukwaConf.get("chukwa.agent.connector");
    if(connector != null && connector.contains("PipelineConnector")){
      _shouldUseConnector = true;
    }
    return s;
  }
}
