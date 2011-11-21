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

package org.apache.hadoop.chukwa.datastore;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.hicc.HiccWebServer;
import org.apache.hadoop.chukwa.rest.bean.ViewBean;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

public class ViewStore {
  private String vid = null;
  private String uid = null;
  private ViewBean view = null;
  private static Log log = LogFactory.getLog(ViewStore.class);
  private static Configuration config = new Configuration();
  private static ChukwaConfiguration chukwaConf = new ChukwaConfiguration();
  private static String viewPath = config.get("fs.default.name")+File.separator+chukwaConf.get("chukwa.data.dir")+File.separator+"hicc"+File.separator+"views";
  private static String publicViewPath = viewPath+File.separator+"public";
  private static String usersViewPath = viewPath+File.separator+"users";
  private static String PUBLIC = "public".intern();
  private static String PRIVATE = "private".intern();

  public ViewStore() throws IllegalAccessException {
    ViewStore.config = HiccWebServer.getConfig();
  }

  public ViewStore(String uid, String vid) throws IllegalAccessException {
    this.uid=uid;
    this.vid=vid;
    load(uid, vid);
  }

  public void load(String uid, String vid) throws IllegalAccessException {
    StringBuilder vp = new StringBuilder();
    vp.append(usersViewPath);
    vp.append(File.separator);
    vp.append(uid);
    vp.append(File.separator);
    vp.append(vid);
    vp.append(".view");
    Path viewFile = new Path(vp.toString());
    try {
      FileSystem fs = FileSystem.get(config);
      if(!fs.exists(viewFile)) {
        StringBuilder pubPath = new StringBuilder();
        pubPath.append(publicViewPath);
        pubPath.append(File.separator);
        pubPath.append(vid);
        pubPath.append(".view");
        viewFile = new Path(pubPath.toString());        
      }
      if(fs.exists(viewFile)) {
        FileStatus[] fstatus = fs.listStatus(viewFile);
        long size = fstatus[0].getLen();
        FSDataInputStream viewStream = fs.open(viewFile);
        byte[] buffer = new byte[(int)size];
        viewStream.readFully(buffer);
        viewStream.close();
        try {
          view = new ViewBean(buffer);
          view.update();
        } catch (Exception e) {
          log.error(ExceptionUtil.getStackTrace(e));
          throw new IllegalAccessException("Unable to access view: "+vid);
        }
      }
    } catch (IOException ex) {
      log.error(ExceptionUtil.getStackTrace(ex));
    }
  }
  
  public String getMessage() {
    return view.toString();  
  }
  
  public ViewBean get() throws IllegalAccessException {
    if(view==null) {
      load(uid, vid);
    }
    if(view==null) {
      // Display global default view if user default view does not exist.
      try {
        load(null, vid);
      } catch(Exception e) {
        log.error(ExceptionUtil.getStackTrace(e));
        view = null;
      }
    }
    return view;
  }
  
  public void set(ViewBean view) throws IllegalAccessException {
    try {
      if (this.view == null || (this.view.getOwner().intern() == view.getOwner().intern())) {
        if(this.view!=null) {
          delete();
        }
        StringBuilder viewPath = new StringBuilder();
        if(view.getPermissionType().intern()==PUBLIC) {
          viewPath.append(publicViewPath);
        } else {
          viewPath.append(usersViewPath);
          viewPath.append(File.separator);
          viewPath.append(uid);
        }
        viewPath.append(File.separator);
        viewPath.append(view.getName());
        viewPath.append(".view");
        Path viewFile = new Path(viewPath.toString());
        try {
          FileSystem fs = FileSystem.get(config);
          FSDataOutputStream out = fs.create(viewFile,true);
          out.write(view.deserialize().toString().getBytes());
          out.close();
        } catch (IOException ex) {
          log.error(ExceptionUtil.getStackTrace(ex));
        }
        this.view = view;
      } else {
        if(view.getPermissionType().intern()==PUBLIC) {
          throw new IllegalAccessException("Unable to save public view, duplicated view exists.");
        } else {
          throw new IllegalAccessException("Unable to save user view.");          
        }
      }
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new IllegalAccessException("Unable to access user view.");
    }
  }

  public void delete() throws IllegalAccessException {
    try {
      if(this.view==null) {
        get();
      }
      if (this.view!=null) {
        StringBuilder viewPath = new StringBuilder();
        if(view.getPermissionType().intern()==PUBLIC) {
          viewPath.append(publicViewPath);
        } else {
          viewPath.append(usersViewPath);
          viewPath.append(File.separator);
          viewPath.append(uid);
        }
        viewPath.append(File.separator);
        viewPath.append(view.getName());
        viewPath.append(".view");
        Path viewFile = new Path(viewPath.toString());
        try {
          FileSystem fs = FileSystem.get(config);
          fs.delete(viewFile, true);
        } catch (IOException ex) {
          log.error(ExceptionUtil.getStackTrace(ex));
        }
      } else {
        throw new IllegalAccessException("Unable to delete user view, view does not exist.");          
      }
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new IllegalAccessException("Unable to access user view.");
    }
  }
  
  public static JSONArray list(String uid) throws IllegalAccessException {
    StringBuilder viewPath = new StringBuilder();
    viewPath.append(usersViewPath);
    viewPath.append(File.separator);
    viewPath.append(uid);
    String[] pathList = new String[2];
    pathList[0]=viewPath.toString();
    pathList[1]=publicViewPath;
    JSONArray list = new JSONArray();
    for(String path : pathList) {
      Path viewFile = new Path(path);
      try {
        FileSystem fs = FileSystem.get(config);
        FileStatus[] fstatus = fs.listStatus(viewFile);
        if(fstatus!=null) {
          for(int i=0;i<fstatus.length;i++) {
            if(!fstatus[i].getPath().getName().endsWith(".view")) {
              continue;
            }
            long size = fstatus[i].getLen();
            FSDataInputStream viewStream = fs.open(fstatus[i].getPath());
            byte[] buffer = new byte[(int)size];
            viewStream.readFully(buffer);
            viewStream.close();
            try {
              ViewBean view = new ViewBean(buffer);
              Map<String, String> json=new LinkedHashMap<String, String>();
              json.put("name", view.getName());
              json.put("type", view.getPermissionType());
              json.put("owner", view.getOwner());
              if(uid.intern()==view.getOwner().intern()) {
                json.put("editable","true");
              } else {
                json.put("editable","false");
              }
              list.add(json);
            } catch (Exception e) {
              log.error(ExceptionUtil.getStackTrace(e));
            }
          }
        }
      } catch (IOException ex) {
        log.error(ExceptionUtil.getStackTrace(ex));
        throw new IllegalAccessException("Unable to access user view."); 
      }
    }
    return list;
  }
}
           
