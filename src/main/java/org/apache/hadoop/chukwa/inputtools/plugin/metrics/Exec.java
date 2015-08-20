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
package org.apache.hadoop.chukwa.inputtools.plugin.metrics;


import java.util.Timer;
import java.util.TimerTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.inputtools.plugin.IPlugin;
import org.json.simple.JSONObject;

public class Exec extends TimerTask {
  private static Log log = LogFactory.getLog(Exec.class);
  private String cmde = null;
  private IPlugin plugin = null;

  public Exec(String[] cmds) {
    StringBuffer c = new StringBuffer();
    for (String cmd : cmds) {
      c.append(cmd);
      c.append(" ");
    }
    cmde = c.toString();
    plugin = new ExecHelper(cmds);
  }

  public void run() {
    try {
      JSONObject result = plugin.execute();
      int status = (Integer) result.get("status");
      if (status < 0) {
        System.out.println("Error");
        log.warn("[ChukwaError]:" + Exec.class + ", "
            + result.get("stderr"));
      } else {
        log.info(result.get("stdout"));
      }
    } catch (Exception e) {
      log.error("Exec output unparsable:" + this.cmde);
    }
  }

  public String getCmde() {
    return cmde;
  }

  public static void main(String[] args) {
    int period = 60;
    try {
      if (System.getProperty("PERIOD") != null) {
        period = Integer.parseInt(System.getProperty("PERIOD"));
      }
    } catch (NumberFormatException ex) {
      ex.printStackTrace();
      System.out
          .println("Usage: java -DPERIOD=nn -DRECORD_TYPE=recordType Exec [cmd]");
      System.out.println("PERIOD should be numeric format of seconds.");
      return;
    }
    Timer timer = new Timer();
    timer.schedule(new Exec(args), 0, period * 1000);
  }
}
