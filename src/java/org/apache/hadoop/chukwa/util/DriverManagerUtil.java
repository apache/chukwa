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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DriverManagerUtil {
  
  @SuppressWarnings("unchecked")
  public static Class loadDriver() throws ClassNotFoundException {
    String jdbcDriver = System.getenv("JDBC_DRIVER");
    if(jdbcDriver == null) {
      jdbcDriver = "com.mysql.jdbc.Driver";
    }
    return Class.forName(jdbcDriver);
  }
  
  public static Connection getConnection(String url) throws SQLException {
    ConnectionInfo info = new ConnectionInfo(url);
    return DriverManager.getConnection(info.getUri(), info.getProperties());
  }
  
  public static class ConnectionInfo {
    private Properties properties = new Properties();
    private String uri = null;
    
    public ConnectionInfo(String url) {
      int pos = url.indexOf('?');
      if(pos == -1) {
        uri = url;
      } else {
        uri = url.substring(0, pos);
        String[] paras = url.substring(pos + 1).split("&");
        for(String s : paras) {
          if(s==null || s.length()==0) {
            continue;
          }
          String[] kv = s.split("=");
          if(kv.length > 1) {
            properties.put(kv[0], kv[1]);
          }
          else {
            properties.put(kv[0], "");
          }
        }
      }
    }

    public Properties getProperties() {
      return properties;
    }

    public String getUri() {
      return uri;
    }
  }
}
