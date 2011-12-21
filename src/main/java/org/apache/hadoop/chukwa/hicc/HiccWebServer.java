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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.jetty.Server;
import org.mortbay.xml.XmlConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HiccWebServer {
  private static Log log = LogFactory.getLog(HiccWebServer.class);
  private static URL serverConf = HiccWebServer.class.getResource("/WEB-INF/jetty.xml");
  private Server server = null;
  private String chukwaHdfs;
  private String hiccData;
  private static HiccWebServer instance = null;
  private static final Configuration config = new Configuration();
  protected static final ChukwaConfiguration chukwaConf = new ChukwaConfiguration();

  protected HiccWebServer() {
  }
 
  public static HiccWebServer getInstance() {
    if(instance==null) {
      instance = new HiccWebServer();
    }
    if(serverConf==null) {
      log.error("Unable to locate jetty-web.xml.");
      DaemonWatcher.bailout(-1);
    }
    return instance;
  }

  public void start() {
    try {
      chukwaHdfs = config.get("fs.default.name")+File.separator+chukwaConf.get("chukwa.data.dir");
      hiccData = chukwaHdfs+File.separator+"hicc";
      DaemonWatcher.createInstance("hicc");
      setupDefaultData();
      run();
    } catch(Exception e) {
      log.error("HDFS unavailable, check configuration in chukwa-env.sh.");
      DaemonWatcher.bailout(-1);
    }
  }

  public static Configuration getConfig() {
    return config;
  }
  
  public List<String> getResourceListing(String path) throws URISyntaxException, IOException {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    URL dirURL = contextClassLoader.getResource(path);

    if (dirURL == null) {
      dirURL = contextClassLoader.getResource(path);
    }
    
    if (dirURL.getProtocol().equals("jar")) {
      /* A JAR path */
      String jarPath = dirURL.getPath().substring(5, dirURL.getPath().indexOf("!")); //strip out only the JAR file
      JarFile jar = new JarFile(jarPath);
      Enumeration<JarEntry> entries = jar.entries(); //gives ALL entries in jar
      List<String> result = new ArrayList<String>(); //avoid duplicates in case it is a subdirectory
      while(entries.hasMoreElements()) {
        String name = entries.nextElement().getName();
        if (name.startsWith(path)) { //filter according to the path
          String entry = name.substring(path.length());
          int checkSubdir = entry.indexOf("/");
          if (checkSubdir == 0 && entry.length()>1) {
            // if it is a subdirectory, we just return the directory name
            result.add(name);
          }
        }
      }
      return result;
    } 
      
    throw new UnsupportedOperationException("Cannot list files for URL "+dirURL);
  }
  
  public void populateDir(List<String> files, Path path) {
    try {
      FileSystem fs = FileSystem.get(config);
      ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      for(String source : files) {
        String name = source.substring(source.indexOf(File.separator));
        Path dest = new Path(path.toString()+File.separator+name);
        InputStream is = contextClassLoader.getResourceAsStream(source);
        StringBuilder sb = new StringBuilder();
        String line = null;
        try {
          BufferedReader reader = new BufferedReader(new InputStreamReader(is));
          while ((line = reader.readLine()) != null) {
            sb.append(line + "\n");
          }
          FSDataOutputStream out = fs.create(dest);
          out.write(sb.toString().getBytes());
          out.close();
          reader.close();
        } catch(IOException e) {
            log.error("Error writing file: "+dest.toString());
        }
      }
    } catch(IOException e) {
      log.error("HDFS unavailable, check configuration in chukwa-env.sh.");
    }
  }
  
  public void setupDefaultData() {
    Path hiccPath = new Path(hiccData);
    try {
      FileSystem fs = FileSystem.get(config);
      if(!fs.exists(hiccPath)) {
        log.info("Initializing HICC Datastore.");
        // Create chukwa directory
        if(!fs.exists(new Path(chukwaHdfs))) {
          fs.mkdirs(new Path(chukwaHdfs));
        }
        
        // Create hicc directory        
        fs.mkdirs(hiccPath);
        
        // Populate widgets repository
        StringBuffer hiccWidgets = new StringBuffer();
        hiccWidgets.append(hiccData);
        hiccWidgets.append(File.separator);
        hiccWidgets.append("widgets");
        Path widgetsPath = new Path(hiccWidgets.toString());
        fs.mkdirs(widgetsPath);
        List<String> widgets = getResourceListing("descriptors");
        populateDir(widgets, widgetsPath);
        
        // Create views directory
        StringBuffer hiccViews = new StringBuffer();
        hiccViews.append(hiccData);
        hiccViews.append(File.separator);
        hiccViews.append("views");        
        fs.mkdirs(new Path(hiccViews.toString()));
        
        // Create users repository
        StringBuffer hiccUsers = new StringBuffer();
        hiccUsers.append(hiccViews);
        hiccUsers.append(File.separator);
        hiccUsers.append("users");
        fs.mkdirs(new Path(hiccUsers.toString()));

        // Populate public repository
        StringBuffer hiccPublic = new StringBuffer();
        hiccPublic.append(hiccViews);
        hiccPublic.append(File.separator);
        hiccPublic.append("public");
        Path viewsPath = new Path(hiccPublic.toString());
        fs.mkdirs(viewsPath);
        List<String> views = getResourceListing("views");
        populateDir(views, viewsPath);
        log.info("HICC Datastore initialization completed.");
      }
    } catch (IOException ex) {
      log.error(ExceptionUtil.getStackTrace(ex));
    } catch (URISyntaxException ex) {
      log.error(ExceptionUtil.getStackTrace(ex));      
    }
  }

  public void run() {
    server = new Server();
    XmlConfiguration configuration;
    try {
      configuration = new XmlConfiguration(serverConf);
      configuration.configure(server);
      server.start();
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }     
  }
  
  public void shutdown() {
    try {
      server.stop();
      DaemonWatcher.bailout(0);
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
  }
  
  public static void main(String[] args) {
    HiccWebServer hicc = HiccWebServer.getInstance();
    hicc.start();
    System.out.close();
    System.err.close();
  }

}
