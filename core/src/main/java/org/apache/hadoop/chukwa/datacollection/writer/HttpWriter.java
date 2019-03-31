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

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class HttpWriter extends PipelineableWriter{
  String _host;
  int _port;
  private Set<String> _whiteListSet = new HashSet<String>();
  private final Logger log = Logger.getLogger(HttpWriter.class);

  @Override
  public void init(Configuration c) throws WriterException {
    _host = c.get("chukwa.http.writer.host", "localhost");
    String port = c.get("chukwa.http.writer.port", "8802");
    String whiteListProp = c.get("chukwa.http.writer.whitelist", "STATUS");
    String[] whiteList = whiteListProp.split(",");
    for(String adaptor:whiteList){
      _whiteListSet.add(adaptor.trim());
    }
    try{
      _port = Integer.parseInt(port);
    } catch(NumberFormatException e){
      throw new WriterException(e);
    }
  }

  @Override
  public void close() throws WriterException {
  }
  
  @Override
  public CommitStatus add(List<Chunk> chunks) throws WriterException {
    CommitStatus rv = ChukwaWriter.COMMIT_OK;
    DataOutputStream dos = null;
    Socket sock = null;
    try{
      sock = new Socket(_host, _port);
      dos = new DataOutputStream(sock.getOutputStream());
      for(Chunk chunk:chunks){
        if(!_whiteListSet.contains(chunk.getStreamName())){
          continue;
        }
        dos.writeInt(chunk.getData().length);
        dos.write(chunk.getData());
        dos.flush();
      }
      log.info("Written chunks");
    } catch(Exception e){
      throw new WriterException(e);
    } finally {
      if(dos != null){
        try {
          dos.close();
        } catch(IOException e) {
          log.error("Error closing dataoutput stream:" + e);
        }
      }
      if (sock != null) {
        try {
          sock.close();
        } catch (IOException e) {
          log.error("Error closing socket: " + e);
        }
      }
      if (next != null) {
        rv = next.add(chunks); //pass data through
      }
    }
    return rv;
  }

}
