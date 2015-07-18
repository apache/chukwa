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

package org.apache.hadoop.chukwa.hicc;


import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.util.ExceptionUtil;

public class Views {
  public JSONArray viewsData;
  private String path = System.getenv("CHUKWA_DATA_DIR")
      + "/views/workspace_view_list.cache";
  private static final Log log = LogFactory.getLog(Views.class);

  static public String getContents(File aFile) {
    // ...checks on aFile are elided
    StringBuffer contents = new StringBuffer();

    try {
      // use buffering, reading one line at a time
      // FileReader always assumes default encoding is OK!
      BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream(aFile.getAbsolutePath()), Charset.forName("UTF-8")));
      try {
        String line = null; // not declared within while loop
        /*
         * readLine is a bit quirky : it returns the content of a line MINUS the
         * newline. it returns null only for the END of the stream. it returns
         * an empty String if two newlines appear in a row.
         */
        while ((line = input.readLine()) != null) {
          contents.append(line);
          contents.append(System.getProperty("line.separator"));
        }
      } finally {
        input.close();
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    }

    return contents.toString();
  }

  public Views() {
    File aFile = new File(path);
    String buffer = getContents(aFile);
    try {
      viewsData = (JSONArray) JSONValue.parse(buffer);
    } catch (Exception e) {
      log.debug(ExceptionUtil.getStackTrace(e));
    }
  }

  public String getOwner(int i) {
    String owner = null;
    try {
      owner = ((JSONObject) viewsData.get(i)).get("owner")
          .toString();
    } catch (Exception e) {
      log.debug(ExceptionUtil.getStackTrace(e));
    }
    return owner;
  }

  public Iterator getPermission(int i) {
    Iterator permission = null;
    try {
      permission = ((JSONObject) ((JSONObject) viewsData.get(i))
          .get("permission")).keySet().iterator();
    } catch (Exception e) {
      log.debug(ExceptionUtil.getStackTrace(e));
    }
    return permission;
  }

  public String getReadPermission(int i, String who) {
    String read = null;
    try {
      JSONObject view = (JSONObject) viewsData.get(i);
      JSONObject permission = (JSONObject) view.get("permission");
      JSONObject user = (JSONObject) permission.get(who);
      read = user.get("read").toString();
    } catch (Exception e) {
      log.debug(ExceptionUtil.getStackTrace(e));
    }
    return read;
  }

  public String getWritePermission(int i, String who) {
    String write = null;
    try {
      write = ((JSONObject) ((JSONObject) ((JSONObject) viewsData.get(i)).get("permission")).get(who)).get("write").toString();
    } catch (Exception e) {
      log.debug(ExceptionUtil.getStackTrace(e));
    }
    return write;
  }

  public String getDescription(int i) {
    String description = null;
    try {
      description = ((JSONObject) viewsData.get(i)).get(
          "description").toString();
    } catch (Exception e) {
      log.debug(ExceptionUtil.getStackTrace(e));
    }
    return description;
  }

  public String getKey(int i) {
    String key = null;
    try {
      key = ((JSONObject) viewsData.get(i)).get("key").toString();
    } catch (Exception e) {
      log.debug(ExceptionUtil.getStackTrace(e));
    }
    return key;
  }

  public int length() {
    return viewsData.size();
  }
}
