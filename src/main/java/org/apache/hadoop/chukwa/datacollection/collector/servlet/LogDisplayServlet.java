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
package org.apache.hadoop.chukwa.datacollection.collector.servlet;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import org.apache.hadoop.chukwa.*;
import org.apache.hadoop.chukwa.datacollection.writer.ExtractorWriter;
import org.apache.hadoop.conf.Configuration;

public class LogDisplayServlet extends HttpServlet {
  
  /*
  static class StreamName {
    byte[] md5;
    public StreamName(Chunk c) {
  
    }
    @Override
    public int hashCode() {
      int x=0;
      for(int i=0; i< md5.length; ++i) {
        x ^= (md5[i] << 4 * i);
      }
      return x;
    }
    
    public boolean equals(Object x) {
      if(x instanceof StreamName)
        return Arrays.equals(md5, ((StreamName)x).md5);
      else return false;
    }
  }*/
  
  public static final String DEFAULT_PATH = "logs";
  public static final String ENABLED_OPT = "chukwaCollector.showLogs.enabled";
  public static final String BUF_SIZE_OPT = "chukwaCollector.showLogs.buffer";
  long BUF_SIZE = 1024* 1024;
  
  Configuration conf;
  Map<String, Deque<Chunk>> chunksBySID = new HashMap<String, Deque<Chunk>>();
  Queue<String> receivedSIDs = new LinkedList<String>();
  long totalStoredSize = 0;

  private static final long serialVersionUID = -4602082382919009285L;
  protected static Logger log = Logger.getLogger(LogDisplayServlet.class);
  
  public LogDisplayServlet() {
    conf = new Configuration();
    ExtractorWriter.recipient = this;
  }
  
  public LogDisplayServlet(Configuration c) {
    conf = c;
    ExtractorWriter.recipient = this;
  }

  public void init(ServletConfig servletConf) throws ServletException {
    BUF_SIZE = conf.getLong(BUF_SIZE_OPT, BUF_SIZE);
  }

  @Override
  protected void doTrace(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException { 
    resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED); 
  }
  
  private String getSID(Chunk c) {
    try { 
      MessageDigest md;
      md = MessageDigest.getInstance("MD5");
  
      md.update(c.getSource().getBytes());
      md.update(c.getStreamName().getBytes());
      md.update(c.getTags().getBytes());
      StringBuilder sb = new StringBuilder();
      byte[] bytes = md.digest();
      for(int i=0; i < bytes.length; ++i) {
        if( (bytes[i] & 0xF0) == 0)
          sb.append('0');
        sb.append( Integer.toHexString(0xFF & bytes[i]) );
      }
      return sb.toString();
    } catch(NoSuchAlgorithmException n) {
      log.fatal(n);
      System.exit(0);
      return null;
    }
  }
  
  
  private void pruneOldEntries() {
    while(totalStoredSize > BUF_SIZE) {
      String queueToPrune = receivedSIDs.remove();
      Deque<Chunk> stream = chunksBySID.get(queueToPrune);
      assert !stream.isEmpty() : " expected a chunk in stream with ID " + queueToPrune;
      Chunk c = stream.poll();
      if(c != null)
        totalStoredSize -= c.getData().length;
      if(stream.isEmpty()) {  //remove empty deques and their names.
        chunksBySID.remove(queueToPrune);
      }
    }
  }
  
  public synchronized void add(List<Chunk> chunks) {
    for(Chunk c : chunks) {
      String sid = getSID(c);
      Deque<Chunk> stream = chunksBySID.get(sid);
      if(stream == null) {
        stream = new LinkedList<Chunk>();
        chunksBySID.put(sid, stream);
      }
      stream.add(c);
      receivedSIDs.add(sid);
      totalStoredSize += c.getData().length;
    }
    pruneOldEntries();
  }
  

  @Override
  protected synchronized void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException  {
  
    PrintStream out = new PrintStream(new BufferedOutputStream(resp.getOutputStream()));
    resp.setStatus(200);
    String path = req.getServletPath();
    String streamID = req.getParameter("sid");
    if (streamID != null) {
      try {
        Deque<Chunk> chunks = chunksBySID.get(streamID);
        if(chunks != null) {
          String streamName = getFriendlyName(chunks.peek());
          out.println("<html><title>Chukwa:Received Data</title><body><h2>Data from "+ streamName + "</h2>");
          out.println("<pre>");
          for(Chunk c: chunks) {
            out.write(c.getData());
          }
          out.println("</pre><hr><a href=\""+path+"\">Back to list of streams</a>");
        } else
          out.println("No data");
      } catch(Exception e) {
        out.println("<html><body>No data</body></html>");
      }
      out.println("</body></html>");
    } else {
      out.println("<html><title>Chukwa:Received Data</title><body><h2>Recently-seen streams</h2><ul>");
      for(Map.Entry<String, Deque<Chunk>> sid: chunksBySID.entrySet()) 
        out.println("<li> <a href=\"" + path + "?sid="+sid.getKey() + "\">"+ getFriendlyName(sid.getValue().peek()) + "</a></li>");
      out.println("</ul></body></html>");
    }
    out.flush();
  }

  private String getFriendlyName(Chunk chunk) {
    if(chunk != null)
      return chunk.getTags() + "/" + chunk.getSource() + "/" + chunk.getStreamName();
    else return "null";
  }


}
