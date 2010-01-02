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
package org.apache.hadoop.chukwa.rest.resource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.dataloader.SocketDataLoader;
import org.apache.hadoop.chukwa.rest.bean.ClientTraceBean;
import org.apache.hadoop.chukwa.util.RecordConstants;

/**
 * Client Trace REST API for parsing client trace log file and convert
 * data into consumable format for web browser and web services.
 */
@Path("clienttrace")
public class ClientTrace {
  protected static Log log = LogFactory.getLog(ClientTrace.class);
  private static SocketDataLoader sdl = null;
  // Client trace log file pattern
  private final Pattern pattern =
    Pattern.compile("(.+?) (.+?),(.+?) (.+?) src\\: /?(.+?):(.+?), dest\\: /?(.+?):(.+?), bytes\\: (\\d+), op\\: (.+?), cli(.+?)");

  /**
   * Get a list of the most recent client trace activities.
   * The extracted elements are:
   * 
   * date   - Timestamp of the activity happened.
   * action - Operation type: HDFS_READ, HDFS_WRITE, or MAPRED_SHUFFLE.
   * src    - Source IP address
   * dest   - Destination IP address
   * size   - Size of the data payload.
   * 
   */
  @GET
  public List<ClientTraceBean> getTrace() {
    if(sdl==null) {
      sdl = new SocketDataLoader("ClientTrace");
    } else if(!sdl.running()) {
      sdl.start();
    }

    List<ClientTraceBean> list = new ArrayList<ClientTraceBean>();
    try {
      Collection<Chunk> clist = sdl.read();
      for(Chunk c : clist) {
        if(c!=null && c.getData()!=null) {
          String action = "";
          long size = 0;
          String data = new String(c.getData());
          String[] entries = data.split("\n");
          for(String entry : entries) {
            Matcher m = pattern.matcher(entry);
            if(m.matches()) {
              ClientTraceBean ctb = new ClientTraceBean();
              size = Long.parseLong(m.group(9));
              action = m.group(10);
              StringBuilder date = new StringBuilder();
              date.append(m.group(1));
              date.append(" ");
              date.append(m.group(2));
              ctb.setDate(date.toString());
              ctb.setSrc(m.group(5));
              ctb.setDest(m.group(7));
              ctb.setAction(action);
              ctb.setSize(size);          
              list.add(ctb);            
            } else {
              log.error("Unparsable line: "+entry);
            }
          }
        }
      }
    } catch(NoSuchElementException e) {
      log.debug("No data available for client trace.");
    }
    
    return list;
  }

}
