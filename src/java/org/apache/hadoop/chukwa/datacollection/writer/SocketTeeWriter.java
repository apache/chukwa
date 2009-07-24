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
package org.apache.hadoop.chukwa.datacollection.writer;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.PatternSyntaxException;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.util.DumpChunks;
import static org.apache.hadoop.chukwa.util.DumpChunks.Filter;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.io.*;

/**
 * Effectively a "Tee" in the writer pipeline.
 * Accepts incoming connections on port specified by chukwaCollector.tee.port.
 * Defaults to 9094
 * 
 * Protocol is as follows:
 * Client ---> TeeWriter   "RAW | WRITABLE <filter>" 
 *                  as per DumpChunks.
 *                  
 * TeeWriter ---> Client "OK\n"                 
 *   In RAW mode               
 * TeeWriter ---> Client (length(int)  byte[length])*
 *              An indefinite sequence of length, followed by byte array.
 *              
 *  In Writable mode
 * TeeWriter ---> Client    (Chunk serialized as Writable)*
 *              An indefinite sequence of serialized chunks
 *              
 *  In english: clients should connect and say either "RAW " or "WRITABLE " 
 *  followed by a filter.  (Note that the keyword is followed by exactly one space.)
 *  They'll then receive either a sequence of byte arrays or of writable-serialized
 *  
 */
public class SocketTeeWriter implements PipelineableWriter {

  public static final String WRITABLE = "WRITABLE";
  public static final String RAW = "RAW";
  static final int DEFAULT_PORT = 9094;
  static Logger log = Logger.getLogger(SocketTeeWriter.class);
  volatile boolean running = true;
  int timeout;
  
  /**
   * Listens for incoming connections, spawns a Tee to deal with each.
   */
  class SocketListenThread extends Thread {
    ServerSocket s;
    public SocketListenThread(Configuration conf) throws IOException {
      int portno = conf.getInt("chukwaCollector.tee.port", DEFAULT_PORT);
      s = new ServerSocket(portno);
    }
    
    public void run() {
      log.info("listen thread started");
      try{
        while(running) {
          Socket sock = s.accept();
          log.info("got connection from " + sock.getInetAddress());
          new Tee(sock);
        }
      } catch(IOException e) {
        
      }
    }
    
    public void shutdown() {
      try{
        s.close();
      } catch(IOException e) {
        
      }
    }
  }
  
  /////////////////Internal class Tee//////////////////////
  /**
   * Manages a single socket connection
   */
  class Tee implements Runnable {
    Socket sock;
    BufferedReader in;
    DataOutputStream out;
    DumpChunks.Filter rules;
    boolean sendRawBytes;
    public Tee(Socket s) throws IOException {
      sock = s;
      //now initialize asynchronously
      run();
//      new Thread(this).start();
    }
    /**
     * initializes the tee.
     */
    public void run() {
      try {   //outer try catches IOExceptions
       try { //inner try catches Pattern Syntax errors
        sock.setSoTimeout(timeout);
        in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
        String cmd = in.readLine();
        if(!cmd.contains(" ")) {
          
          throw new PatternSyntaxException(
              "command should be keyword pattern, but no ' ' seen", cmd, -1);
        }
        String uppercased = cmd.substring(0, cmd.indexOf(' ')).toUpperCase();
        if(RAW.equals(uppercased))
          sendRawBytes = true;
        else if(!WRITABLE.equals(uppercased)) {
          throw new PatternSyntaxException("bad command '" + uppercased+
              "' -- starts with neither '"+ RAW+ "' nor '"+ WRITABLE+"'.", cmd, -1);
        }
        
        String cmdAfterSpace = cmd.substring(cmd.indexOf(' ')+1);
        rules = new DumpChunks.Filter(cmdAfterSpace);
        out = new DataOutputStream(sock.getOutputStream());

          //now that we read everything OK we can add ourselves to list, and return.
        synchronized(tees) {
          tees.add(this);
        }
        out.write("OK\n".getBytes());
        log.info("tee to " + sock.getInetAddress() + " established");
      } catch(PatternSyntaxException e) {
          out.write(e.toString().getBytes());
          out.writeByte('\n');
          out.close();
          in.close();
          sock.close();
          log.warn(e);
        }//end inner catch
      } catch(IOException e) { //end outer catch
         log.warn(e);
      }
    }
    
    public void maybeSend(Chunk c) throws IOException {
      if(rules.matches(c)) {
        if(sendRawBytes) {
          byte[] data = c.getData();
          out.writeInt(data.length);
          out.write(data);
        } else
          c.write(out);
      }
    }
    
    public void close() {
      try {
        out.close();
        in.close();
      } catch(Exception e) {}
    }
  }
  
  
  /////////////////Main class SocketTeeWriter//////////////////////
  
  
  SocketListenThread listenThread;
  List<Tee> tees;
  ChukwaWriter next;
  
  @Override
  public void setNextStage(ChukwaWriter next) {
    this.next = next;
  }

  @Override
  public void add(List<Chunk> chunks) throws WriterException {
    next.add(chunks); //pass data through
    synchronized(tees) {
      Iterator<Tee> loop = tees.iterator();
      while(loop.hasNext()) {
        Tee t = loop.next();
        try {
          for(Chunk c: chunks) {
            t.maybeSend(c);
          }
        } catch(IOException e) {
          t.close();
          loop.remove(); //drop failed tee from list.
          log.info("lost connection: "+ e.toString());
        }
      }
    }
  }

  @Override
  public void close() throws WriterException {
    next.close();
    running = false;
    listenThread.shutdown();
  }

  @Override
  public void init(Configuration c) throws WriterException {
    try {
      listenThread = new SocketListenThread(c);
      listenThread.start();
    } catch (IOException e) {
      throw new WriterException(e);
    }
    tees = new ArrayList<Tee>();
  }

}
