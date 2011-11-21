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


import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.inputtools.plugin.ExecPlugin;
import org.apache.log4j.Logger;
import org.apache.log4j.helpers.ISO8601DateFormat;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.*;

/**
 * Runs a command inside chukwa. Takes as params the interval in seconds at
 * which to run the command, and the path and args to execute.
 * 
 * Interval is optional, and defaults to 5 seconds.
 * 
 * Example usage: add
 * org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Ps 2 /bin/ps aux
 * 0
 * 
 */
public class ExecAdaptor extends AbstractAdaptor {
  
  public static final boolean FULL_PATHS = false;
  
  static class EmbeddedExec extends ExecPlugin {

    String cmd;
    
    public EmbeddedExec(String c) {
      cmd = c;
    }
    
    @Override
    public String getCmde() {
      return cmd;
    }
  }
  
  EmbeddedExec exec;
  static final boolean FAKE_LOG4J_HEADER = true;
  static final boolean SPLIT_LINES = false;
  static Logger log = Logger.getLogger(ExecAdaptor.class);
  
  class RunToolTask extends TimerTask {
    public void run() {
      log.info("calling exec");
      JSONObject o = exec.execute();
      try {

        if (o.getInt("status") == exec.statusKO) {
          deregisterAndStop();
          return;
        }

        // FIXME: downstream customers would like timestamps here.
        // Doing that efficiently probably means cutting out all the
        // excess buffer copies here, and appending into an OutputBuffer.
        byte[] data;
        if (FAKE_LOG4J_HEADER) {
          StringBuilder result = new StringBuilder();
          ISO8601DateFormat dateFormat = new org.apache.log4j.helpers.ISO8601DateFormat();
          result.append(dateFormat.format(new java.util.Date()));
          result.append(" INFO org.apache.hadoop.chukwa.");
          result.append(type);
          result.append("= ");
          result.append(o.getString("exitValue"));
          result.append(": ");
          result.append(o.getString("stdout"));
          data = result.toString().getBytes();
        } else {
          String stdout = o.getString("stdout");
          data = stdout.getBytes();
        }

        sendOffset += data.length;
        ChunkImpl c = new ChunkImpl(ExecAdaptor.this.type, "results from "
            + cmd, sendOffset, data, ExecAdaptor.this);

        if (SPLIT_LINES) {
          ArrayList<Integer> carriageReturns = new ArrayList<Integer>();
          for (int i = 0; i < data.length; ++i)
            if (data[i] == '\n')
              carriageReturns.add(i);

          c.setRecordOffsets(carriageReturns);
        } // else we get default one record


        //We can't replay exec data, so we might as well commit to it now.
        control.reportCommit(ExecAdaptor.this, sendOffset);
        dest.add(c);
      } catch (JSONException e) {
        log.warn(e);
      } catch (InterruptedException e) {
        ;
      } 
    }
  };

  String cmd;
  final java.util.Timer timer;
  long period = 5;
  volatile long sendOffset = 0;

  public ExecAdaptor() {
    timer = new java.util.Timer();
  }

  @Override
  public String getCurrentStatus() {
    return type + " " + period + " " + cmd;
  }
 
   @Override
   public long shutdown(AdaptorShutdownPolicy shutdownPolicy)
       throws AdaptorException {
     log.info("Enter Shutdown:" + shutdownPolicy.name()+ " - ObjectId:" + this);
     switch(shutdownPolicy) {
     case GRACEFULLY :
     case WAIT_TILL_FINISHED :
       try {
         timer.cancel();
         exec.waitFor();
       } catch (InterruptedException e) {
      }
      break;   
      default:
        timer.cancel();
        exec.stop();
        break;
    }
    log.info("Exist Shutdown:" + shutdownPolicy.name()+ " - ObjectId:" + this);
    return sendOffset;
  }

  @Override
  public void start(long offset) throws AdaptorException {
    if(FULL_PATHS && !(new java.io.File(cmd)).exists())
      throw new AdaptorException("Can't start ExecAdaptor. No command " + cmd);
    this.sendOffset = offset;
    this.exec = new EmbeddedExec(cmd);
    TimerTask execTimer = new RunToolTask();
    timer.schedule(execTimer, 0L, period*1000L);
  }


  @Override
  public String parseArgs(String status) { 
    int spOffset = status.indexOf(' ');
    if (spOffset > 0) {
      try {
        period = Integer.parseInt(status.substring(0, spOffset));
        cmd = status.substring(spOffset + 1);
      } catch (NumberFormatException e) {
        log.warn("ExecAdaptor: sample interval "
            + status.substring(0, spOffset) + " can't be parsed");
        cmd = status;
      }
    } else
      cmd = status;
    
    return cmd;
  }


}
