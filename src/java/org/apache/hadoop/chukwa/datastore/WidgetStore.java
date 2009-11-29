package org.apache.hadoop.chukwa.datastore;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.hicc.HiccWebServer;
import org.apache.hadoop.chukwa.rest.bean.CatalogBean;
import org.apache.hadoop.chukwa.rest.bean.WidgetBean;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WidgetStore {
  private static Log log = LogFactory.getLog(WidgetStore.class);
  private static Configuration config = new Configuration();
  private static ChukwaConfiguration chukwaConf = new ChukwaConfiguration();
  private static String hiccPath = config.get("fs.default.name")+File.separator+chukwaConf.get("chukwa.data.dir")+File.separator+"hicc"+File.separator+"widgets";
  private static CatalogBean catalog = null;
  private static HashMap<String, WidgetBean> list = new HashMap<String, WidgetBean>();
  
  public WidgetStore() throws IllegalAccessException {
    WidgetStore.config = HiccWebServer.getConfig();
  }

  public void set(WidgetBean widget) throws IllegalAccessException {
    try {
      StringBuilder widgetPath = new StringBuilder();
      widgetPath.append(hiccPath);
      widgetPath.append(File.separator);
      widgetPath.append(widget.getId());
      widgetPath.append(".descriptor");
      Path widgetFile = new Path(widgetPath.toString());
      FileSystem fs;
      try {
        fs = FileSystem.get(config);
        FSDataOutputStream out = fs.create(widgetFile,true);
        out.writeUTF(widget.deserialize().toString());
        out.close();
      } catch (IOException ex) {
        log.error(ExceptionUtil.getStackTrace(ex));
      }
      cacheWidgets();
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new IllegalAccessException("Unable to access user view database.");
    }
  }
  
  public static void cacheWidgets() throws IllegalAccessException {
    StringBuilder widgetPath = new StringBuilder();
    widgetPath.append(hiccPath);
    Path widgetFiles = new Path(widgetPath.toString());
    FileSystem fs;
    catalog = new CatalogBean();
    catalog.setId("root");
    catalog.setLabel("root");
    try {
      fs = FileSystem.get(config);
      FileStatus[] fstatus = fs.listStatus(widgetFiles);
      if(fstatus!=null) {
        for(int i=0;i<fstatus.length;i++) {
          long size = fstatus[i].getLen();
          FSDataInputStream widgetStream = fs.open(fstatus[i].getPath());
          byte[] buffer = new byte[(int)size];
          widgetStream.readFully(buffer);
          widgetStream.close();
          try {
            WidgetBean widget = new WidgetBean(new JSONObject(new String(buffer)));
            catalog.addCatalog(widget);
            list.put(widget.getId(),widget);
          } catch (Exception e) {
            log.error(ExceptionUtil.getStackTrace(e));
          }
        }
      }
    } catch (IOException ex) {
      log.error(ExceptionUtil.getStackTrace(ex));
      throw new IllegalAccessException("Unable to access user view database."); 
    }    
  }

  public static CatalogBean getCatalog() throws IllegalAccessException {
    if(catalog==null) {
      cacheWidgets();
    }
    return catalog;
  }
  
  public static HashMap<String, WidgetBean> list() throws IllegalAccessException {
    if(list.size()==0) {
      cacheWidgets();
    }
    return list;
  }
}
           
