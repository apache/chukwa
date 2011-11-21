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
package org.apache.hadoop.chukwa.inputtools.plugin.pbsnode;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.inputtools.mdl.DataConfig;
import org.apache.hadoop.chukwa.inputtools.plugin.ExecPlugin;
import org.apache.hadoop.chukwa.inputtools.plugin.IPlugin;
import org.json.JSONException;
import org.json.JSONObject;

public class PbsNodePlugin extends ExecPlugin {
  private static Log log = LogFactory.getLog(PbsNodePlugin.class);
  private String cmde = null;
  private DataConfig dataConfig = null;

  public PbsNodePlugin() {
    dataConfig = new DataConfig();
    cmde = dataConfig.get("chukwa.inputtools.plugin.pbsNode.cmde");
  }

  @Override
  public String getCmde() {
    return cmde;
  }

  public static void main(String[] args) throws JSONException {
    IPlugin plugin = new PbsNodePlugin();
    JSONObject result = plugin.execute();
    System.out.print("Result: " + result);

    if (result.getInt("status") < 0) {
      System.out.println("Error");
      log.warn("[ChukwaError]:" + PbsNodePlugin.class + ", "
          + result.getString("stderr"));
    } else {
      log.info(result.get("stdout"));
    }
  }
}
