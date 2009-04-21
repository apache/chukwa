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

package org.apache.hadoop.chukwa.conf;


import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class ChukwaConfiguration extends Configuration {
  static Logger log = Logger.getLogger(ChukwaConfiguration.class);

  public ChukwaConfiguration() {
    this(true);
  }

  public ChukwaConfiguration(boolean loadDefaults) {
    super();
    if (loadDefaults) {

      String chukwaHome = System.getenv("CHUKWA_HOME");
      if (chukwaHome == null) {
        chukwaHome = ".";
      }

      if (!chukwaHome.endsWith("/")) {
        chukwaHome = chukwaHome + File.separator;
      }
      String chukwaConf = System.getenv("CHUKWA_CONF_DIR");
      if (chukwaConf == null) {
        chukwaConf = chukwaHome + "conf" + File.separator;
      }

      log.info("chukwaConf is " + chukwaConf);

      
      // Hadoop config
      String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
      if (hadoopConfDir != null) {        
        super.addResource(new Path(hadoopConfDir + "/hadoop-site.xml"));
        log.debug("added " + hadoopConfDir"/hadoop-site.xml to ChukwaConfiguration");
      } else {
        String hadoopHome = System.getenv("HADOOP_HOME");
        if (hadoopHome != null) {
          super.addResource(new Path(hadoopHome + "/conf/hadoop-site.xml"));
          log.debug("added " + hadoopHome + "/hadoop-site.xml to ChukwaConfiguration");
        }
      }


      super.addResource(new Path(chukwaConf + "/chukwa-agent-conf.xml"));
      log.debug("added chukwa-agent-conf.xml to ChukwaConfiguration");

      super.addResource(new Path(chukwaConf + "/chukwa-collector-conf.xml"));
      log.debug("added chukwa-collector-conf.xml to ChukwaConfiguration");

      super.addResource(new Path(chukwaConf + "/chukwa-demux-conf.xml"));
      log.debug("added chukwa-demux-conf.xml to ChukwaConfiguration");
    }
  }

}
