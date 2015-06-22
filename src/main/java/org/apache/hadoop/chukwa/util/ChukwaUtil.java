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

package org.apache.hadoop.chukwa.util;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/*
 * Create a common set of utility classes for code reuse
 */

public class ChukwaUtil {

  private static Logger log = Logger.getLogger(ChukwaUtil.class);

  public static Configuration readConfiguration() {
    Configuration conf = new Configuration();

    String chukwaHomeName = System.getenv("CHUKWA_HOME");
    if (chukwaHomeName == null) {
      chukwaHomeName = "";
    }
    File chukwaHome = new File(chukwaHomeName).getAbsoluteFile();

    log.info("Config - CHUKWA_HOME: [" + chukwaHome.toString() + "]");

    String chukwaConfName = System.getProperty("CHUKWA_CONF_DIR");
    File chukwaConf;
    if (chukwaConfName != null)
      chukwaConf = new File(chukwaConfName).getAbsoluteFile();
    else
      chukwaConf = new File(chukwaHome, "conf");

    log.info("Config - CHUKWA_CONF_DIR: [" + chukwaConf.toString() + "]");
    File agentConf = new File(chukwaConf, "chukwa-agent-conf.xml");
    conf.addResource(new Path(agentConf.getAbsolutePath()));
    if (conf.get("chukwaAgent.checkpoint.dir") == null)
      conf.set("chukwaAgent.checkpoint.dir",
          new File(chukwaHome, "var").getAbsolutePath());
    conf.set("chukwaAgent.initial_adaptors", new File(chukwaConf,
        "initial_adaptors").getAbsolutePath());
    try {
      Configuration chukwaAgentConf = new Configuration(false);
      chukwaAgentConf.addResource(new Path(agentConf.getAbsolutePath()));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return conf;
  }

}
