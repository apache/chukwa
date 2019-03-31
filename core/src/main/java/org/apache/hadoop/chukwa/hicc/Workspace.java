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

import javax.servlet.*;
import javax.servlet.http.*;

import java.sql.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.util.XssFilter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.apache.hadoop.chukwa.util.ExceptionUtil;

public class Workspace extends HttpServlet {
  public static final long serialVersionUID = 101L;
  private static final Log log = LogFactory.getLog(Workspace.class);
  private String path = System.getenv("CHUKWA_DATA_DIR");
  private JSONObject hash = new JSONObject();
  transient private XssFilter xf;

  @Override  
  protected void doTrace(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED); 
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    xf = new XssFilter(request);
    response.setContentType("text/plain");
    String method = xf.getParameter("method");
    if (method.equals("get_views_list")) {
      getViewsList(request, response);
    }
    if (method.equals("get_view")) {
      getView(request, response);
    }
    if (method.equals("save_view")) {
      saveView(request, response);
    }
    if (method.equals("change_view_info")) {
      changeViewInfo(request, response);
    }
    if (method.equals("get_widget_list")) {
      getWidgetList(request, response);
    }
    if (method.equals("clone_view")) {
      cloneView(request, response);
    }
    if (method.equals("delete_view")) {
      deleteView(request, response);
    }
  }

  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    doGet(request, response);
  }

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

  public void setContents(String fName, String buffer) {
    try {
      BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fName), Charset.forName("UTF-8")));
      out.write(buffer);
      out.close();
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
    }
  }

  public void cloneView(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    PrintWriter out = response.getWriter();
    String name = xf.getParameter("name");
    String template = xf.getParameter("clone_name");
    File aFile = new File(path + "/views/" + template);
    String config = getContents(aFile);
    int i = 0;
    boolean check = true;
    while (check) {
      String tmpName = name;
      if (i > 0) {
        tmpName = name + i;
      }
      File checkFile = new File(path + "/views/" + tmpName + ".view");
      check = checkFile.exists();
      if (!check) {
        name = tmpName;
      }
      i = i + 1;
    }
    setContents(path + "/views/" + name + ".view", config);
    File deleteCache = new File(path + "/views/workspace_view_list.cache");
    if(!deleteCache.delete()) {
      log.warn("Can not delete "+path + "/views/workspace_view_list.cache");
    }
    genViewCache(path + "/views");
    aFile = new File(path + "/views/workspace_view_list.cache");
    String viewsCache = getContents(aFile);
    out.println(viewsCache);
  }

  public void deleteView(HttpServletRequest request,
      HttpServletResponse response) throws IOException, ServletException {
    String name = xf.getParameter("name");
    File aFile = new File(path + "/views/" + name + ".view");
    if(!aFile.delete()) {
      log.warn("Can not delete " + path + "/views/" + name + ".view");
    }
    File deleteCache = new File(path + "/views/workspace_view_list.cache");
    if(!deleteCache.delete()) {
      log.warn("Can not delete "+path + "/views/workspace_view_list.cache");
    }
    genViewCache(path + "/views");
  }

  public void getViewsList(HttpServletRequest request,
      HttpServletResponse response) throws IOException, ServletException {
    PrintWriter out = response.getWriter();
    genViewCache(path + "/views");
    File aFile = new File(path + "/views/workspace_view_list.cache");
    String viewsCache = getContents(aFile);
    out.println(viewsCache);
  }

  public void getView(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    PrintWriter out = response.getWriter();
    String id = xf.getParameter("id");
    genViewCache(path + "/views");
    File aFile = new File(path + "/views/" + id + ".view");
    String view = getContents(aFile);
    out.println(view);
  }

  public void changeViewInfo(HttpServletRequest request,
      HttpServletResponse response) throws IOException, ServletException {
    PrintWriter out = response.getWriter();
    String id = xf.getParameter("name");
    String config = request.getParameter("config");
    try {
      JSONObject jt = (JSONObject) JSONValue.parse(config);
      File aFile = new File(path + "/views/" + id + ".view");
      String original = getContents(aFile);
      JSONObject updateObject = (JSONObject) JSONValue.parse(original);
      updateObject.put("description", jt.get("description"));
      setContents(path + "/views/" + id + ".view", updateObject.toString());
      if (!rename(id, jt.get("description").toString())) {
        throw new Exception("Rename view file failed");
      }
      File deleteCache = new File(path + "/views/workspace_view_list.cache");
      if(!deleteCache.delete()) {
        log.warn("Can not delete "+path + "/views/workspace_view_list.cache");
      }
      genViewCache(path + "/views");
      out.println("Workspace is stored successfully.");
    } catch (Exception e) {
      out.println("Workspace store failed.");
    }
  }

  public void saveView(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    PrintWriter out = response.getWriter();
    String id = xf.getParameter("name");
    String config = request.getParameter("config");
    setContents(path + "/views/" + id + ".view", config);
    out.println("Workspace is stored successfully.");
  }

  public void getWidgetList(HttpServletRequest request,
      HttpServletResponse response) throws IOException, ServletException {
    PrintWriter out = response.getWriter();
    genWidgetCache(path + "/descriptors");
    File aFile = new File(path + "/descriptors/workspace_plugin.cache");
    String viewsCache = getContents(aFile);
    out.println(viewsCache);
  }

  private void genViewCache(String source) {
    File cacheFile = new File(source + "/workspace_view_list.cache");
    if (!cacheFile.exists()) {
      File dir = new File(source);
      File[] filesWanted = dir.listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          return name.endsWith(".view");
        }
      });
      if(filesWanted!=null) {
        JSONObject[] cacheGroup = new JSONObject[filesWanted.length];
        for (int i = 0; i < filesWanted.length; i++) {
          String buffer = getContents(filesWanted[i]);
          try {
            JSONObject jt = (JSONObject) JSONValue.parse(buffer);
            String fn = filesWanted[i].getName();
            jt.put("key", fn.substring(0, (fn.length() - 5)));
            cacheGroup[i] = jt;
          } catch (Exception e) {
            log.debug(ExceptionUtil.getStackTrace(e));
          }
        }
        String viewList = convertObjectsToViewList(cacheGroup);
        setContents(source + "/workspace_view_list.cache", viewList);
      }
    }
  }

  public String convertObjectsToViewList(JSONObject[] objArray) {
    JSONArray jsonArr = new JSONArray();
    JSONObject permission = new JSONObject();
    JSONObject user = new JSONObject();
    try {
      permission.put("read", 1);
      permission.put("modify", 1);
      user.put("all", permission);
    } catch (Exception e) {
      System.err.println("JSON Exception: " + e.getMessage());
    }
    for (int i = 0; i < objArray.length; i++) {
      try {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("key", objArray[i].get("key"));
        jsonObj.put("description", objArray[i].get("description"));
        jsonObj.put("owner", "");
        jsonObj.put("permission", user);
        jsonArr.add(jsonObj);
      } catch (Exception e) {
        System.err.println("JSON Exception: " + e.getMessage());
      }
    }
    return jsonArr.toString();
  }

  private void genWidgetCache(String source) {
    File cacheFile = new File(source + "/workspace_plugin.cache");
    File cacheDir = new File(source);
    if (!cacheFile.exists()
        || cacheFile.lastModified() < cacheDir.lastModified()) {
      File dir = new File(source);
      File[] filesWanted = dir.listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          return name.endsWith(".descriptor");
        }
      });
      if(filesWanted!=null) {
        JSONObject[] cacheGroup = new JSONObject[filesWanted.length];
        for (int i = 0; i < filesWanted.length; i++) {
          String buffer = getContents(filesWanted[i]);
          try {
            JSONObject jt = (JSONObject) JSONValue.parse(buffer);
            cacheGroup[i] = jt;
          } catch (Exception e) {
            log.debug(ExceptionUtil.getStackTrace(e));
          }
        }
        String widgetList = convertObjectsToWidgetList(cacheGroup);
        setContents(source + "/workspace_plugin.cache", widgetList);
      }
    }
  }

  public String convertObjectsToWidgetList(JSONObject[] objArray) {
    JSONObject jsonObj = new JSONObject();
    JSONArray jsonArr = new JSONArray();
    for (int i = 0; i < objArray.length; i++) {
      jsonArr.add(objArray[i]);
    }
    try {
      jsonObj.put("detail", jsonArr);
    } catch (Exception e) {
      System.err.println("JSON Exception: " + e.getMessage());
    }
    for (int i = 0; i < objArray.length; i++) {
      try {
        String[] categoriesArray = objArray[i].get("categories").toString()
            .split(",");
        hash = addToHash(hash, categoriesArray, objArray[i]);
      } catch (Exception e) {
        System.err.println("JSON Exception: " + e.getMessage());
      }
    }
    try {
      jsonObj.put("children", hash);
    } catch (Exception e) {
      System.err.println("JSON Exception: " + e.getMessage());
    }
    return jsonObj.toString();
  }

  public JSONObject addToHash(JSONObject hash, String[] categoriesArray,
      JSONObject obj) {
    JSONObject subHash = hash;
    for (int i = 0; i < categoriesArray.length; i++) {
      String id = categoriesArray[i];
      if (i >= categoriesArray.length - 1) {
        try {
          subHash.put("leaf:" + obj.get("title"), obj.get("id"));
        } catch (Exception e) {
          System.err.println("JSON Exception: " + e.getMessage());
        }
      } else {
        try {
          subHash = (JSONObject) subHash.get("node:" + id);
        } catch (Exception e) {
          try {
            JSONObject tmpHash = new JSONObject();
            subHash.put("node:" + id, tmpHash);
            subHash = tmpHash;
          } catch (Exception ex) {
            log.debug(ExceptionUtil.getStackTrace(e));
          }
        }
      }
    }
    return hash;
  }

  private boolean rename(String id, String desc) {
    try {
      File view = new File(path + "/views/" + id + ".view");
      File newFile = new File(path + File.separator + "views" + File.separator
          + desc + ".view");
      if(!view.renameTo(newFile)) {
        log.warn("Can not rename " + path + "/views/" + id + ".view to " + 
            path + File.separator + "views" + File.separator + desc + ".view");
      }
    } catch (Exception e) {
      return false;
    }
    return true;
  }

}
