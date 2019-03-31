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


import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter;
import org.apache.hadoop.chukwa.datacollection.writer.SeqFileWriter;
import org.apache.hadoop.chukwa.datacollection.writer.WriterException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

@Deprecated
public class ServletCollector extends HttpServlet {

  static final boolean FANCY_DIAGNOSTICS = false;
  public static final String PATH = "chukwa";
  /**
   * If a chunk is committed; then the ack will start with the following string.
   */
  public static final String ACK_PREFIX = "ok: ";
  transient ChukwaWriter writer = null;

  private static final long serialVersionUID = 6286162898591407111L;
  transient Logger log = Logger.getLogger(ServletCollector.class);
  
  boolean COMPRESS;
  String CODEC_NAME;
  transient CompressionCodec codec;

  public void setWriter(ChukwaWriter w) {
    writer = w;
  }
  
  public ChukwaWriter getWriter() {
    return writer;
  }

  long statTime = 0L;
  int numberHTTPConnection = 0;
  int numberchunks = 0;
  long lifetimechunks = 0;

  transient Configuration conf;

  public ServletCollector(Configuration c) {
    conf = c;
  }

  public void init(ServletConfig servletConf) throws ServletException {

    log.info("initing servletCollector");
    if (servletConf == null) {
      log.fatal("no servlet config");
      return;
    }

    Timer statTimer = new Timer();
    statTimer.schedule(new TimerTask() {
      public void run() {
        log.info("stats:ServletCollector,numberHTTPConnection:"
            + numberHTTPConnection + ",numberchunks:" + numberchunks);
        statTime = System.currentTimeMillis();
        numberHTTPConnection = 0;
        numberchunks = 0;
      }
    }, (1000), (60 * 1000));

    if (writer != null) {
      log.info("writer set up statically, no need for Collector.init() to do it");
      return;
    }

    try {
      String writerClassName = conf.get("chukwaCollector.writerClass",
          SeqFileWriter.class.getCanonicalName());
      Class<?> writerClass = Class.forName(writerClassName);
      if (writerClass != null
          && ChukwaWriter.class.isAssignableFrom(writerClass))
        writer = (ChukwaWriter) writerClass.newInstance();
    } catch (Exception e) {
      log.warn("failed to use user-chosen writer class, defaulting to SeqFileWriter", e);
    }

    COMPRESS = conf.getBoolean("chukwaAgent.output.compress", false);
    if( COMPRESS) {
	    CODEC_NAME = conf.get( "chukwaAgent.output.compression.type", "org.apache.hadoop.io.compress.DefaultCodec");
	    Class<?> codecClass = null;
	    try {
			codecClass = Class.forName( CODEC_NAME);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
			log.info("codec " + CODEC_NAME + " loaded for network compression");
		} catch (ClassNotFoundException e) {
			log.warn("failed to create codec " + CODEC_NAME + ". Network compression won't be enabled.", e);
			COMPRESS = false;
		}
    }
    
    // We default to here if the pipeline construction failed or didn't happen.
    try {
      if (writer == null) {
        writer =  new SeqFileWriter();
      }
      
      writer.init(conf);
    } catch (Throwable e) {
      log.warn("Exception trying to initialize SeqFileWriter",e);
    }
  }

  @Override
  protected void doTrace(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException { 
    resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED); 
  }

  protected void accept(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException {
    numberHTTPConnection++;
    final long currentTime = System.currentTimeMillis();
    try {

      log.debug("new post from " + req.getRemoteHost() + " at " + currentTime);
      java.io.InputStream in = req.getInputStream();

      ServletOutputStream l_out = resp.getOutputStream();
      
      DataInputStream di = null;
      boolean compressNetwork = COMPRESS;
      if( compressNetwork){
          InputStream cin = codec.createInputStream( in);
          di = new DataInputStream(cin);
      }
      else {
    	  di = new DataInputStream(in);
      }

      final int numEvents = di.readInt();
      // log.info("saw " + numEvents+ " in request");

      List<Chunk> events = new LinkedList<Chunk>();
      StringBuilder sb = new StringBuilder();

      for (int i = 0; i < numEvents; i++) {
        ChunkImpl logEvent = ChunkImpl.read(di);
        events.add(logEvent);

      }

      int responseStatus = HttpServletResponse.SC_OK;

      // write new data to data sync file
      if (writer != null) {
        ChukwaWriter.CommitStatus result = writer.add(events);

        // this is where we ACK this connection

        if(result == ChukwaWriter.COMMIT_OK) {
          // only count the chunks if result is commit or commit pending
          numberchunks += events.size();
          lifetimechunks += events.size();

          for(Chunk receivedChunk: events) {
            sb.append(ACK_PREFIX);
            sb.append(receivedChunk.getData().length);
            sb.append(" bytes ending at offset ");
            sb.append(receivedChunk.getSeqID() - 1).append("\n");
          }
        } else if(result instanceof ChukwaWriter.COMMIT_PENDING) {

          // only count the chunks if result is commit or commit pending
          numberchunks += events.size();
          lifetimechunks += events.size();

          for(String s: ((ChukwaWriter.COMMIT_PENDING) result).pendingEntries)
            sb.append(s);
        } else if(result == ChukwaWriter.COMMIT_FAIL) {
          sb.append("Commit failed");
          responseStatus = HttpServletResponse.SC_SERVICE_UNAVAILABLE;
        }

        l_out.print(sb.toString());
      } else {
        l_out.println("can't write: no writer");
      }

      resp.setStatus(responseStatus);

    } catch (Throwable e) {
      log.warn("Exception talking to " + req.getRemoteHost() + " at t="
          + currentTime, e);
      throw new ServletException(e);
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    accept(req, resp);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {


    log.info("new GET from " + req.getRemoteHost() + " at " + System.currentTimeMillis());
    PrintStream out = new PrintStream(resp.getOutputStream(), true, "UTF-8");
    resp.setStatus(200);

    String pingAtt = req.getParameter("ping");
    if (pingAtt != null) {
      out.println("Date:" + statTime);
      out.println("Now:" + System.currentTimeMillis());
      out.println("numberHTTPConnection in time window:"
          + numberHTTPConnection);
      out.println("numberchunks in time window:" + numberchunks);
      out.println("lifetimechunks:" + lifetimechunks);
    } else {
      out.println("<html><body><h2>Chukwa servlet running</h2>");
      out.println("</body></html>");
    }

  }

  @Override
  public String getServletInfo() {
    return "Chukwa Servlet Collector";
  }

  @Override
  public void destroy() {
    try {
      writer.close();
    } catch (WriterException e) {
      log.warn("Exception during close", e);
      e.printStackTrace();
    }
    super.destroy();
  }
}
