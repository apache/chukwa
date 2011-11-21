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

package org.apache.hadoop.chukwa.inputtools.mdl;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.io.File;
import java.io.FilenameFilter;

public class DataConfig {
  private static Configuration config;
  final static String MDL_XML = "mdl.xml";
  private Log log = LogFactory.getLog(DataConfig.class);

  public DataConfig(String path) {
    Path fileResource = new Path(path);
    config = new Configuration();
    config.addResource(fileResource);
  }

  public DataConfig() {
    String dataConfig = System.getenv("CHUKWA_CONF_DIR");
    if (dataConfig == null) {
      dataConfig = MDL_XML;
    } else {
      dataConfig += File.separator + MDL_XML;
    }
    log.debug("DATACONFIG=" + dataConfig);
    if (config == null) {
      try {
        Path fileResource = new Path(dataConfig);
        config = new Configuration();
        config.addResource(fileResource);
      } catch (Exception e) {
        log.debug("Error reading configuration file:" + dataConfig);
      }
    }

    if (System.getenv("CHUKWA_CONF_DIR") != null) {
      // Allow site-specific MDL files to be included in the
      // configuration so as to keep the "main" mdl.xml pure.
      File confDir = new File(System.getenv("CHUKWA_CONF_DIR"));
      File[] confFiles = confDir.listFiles(new FilenameFilter() {

        @Override
        public boolean accept(File dir, String name) {
          // Implements a naming convention of ending with "mdl.xml"
          // but is careful not to pick up mdl.xml itself again.
          return name.endsWith(MDL_XML) && !name.equals(MDL_XML);
        }

      });

      if (confFiles != null) {
        for (File confFile : confFiles)
          config.addResource(new Path(confFile.getAbsolutePath()));
      }
    }  
  }

  public String get(String key) {
    return config.get(key);
  }

  public void put(String key, String value) {
    config.set(key, value);
  }

  public Iterator<Map.Entry<String, String>> iterator() {
    return config.iterator();
  }

  public HashMap<String, String> startWith(String key) {
    HashMap<String, String> transformer = new HashMap<String, String>();
    Iterator<Map.Entry<String, String>> entries = config.iterator();
    while (entries.hasNext()) {
      String entry = entries.next().toString();
      if (entry.startsWith(key)) {
        String[] metrics = entry.split("=");
        transformer.put(metrics[0], metrics[1]);
      }
    }
    return transformer;
  }
}
