package org.apache.hadoop.chukwa.hicc;

import java.io.File;
import java.net.URL;

import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandler;
import org.mortbay.xml.XmlConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HiccWebServer {
  private static Log log = LogFactory.getLog(HiccWebServer.class);
  private static URL serverConf = null;
  private Server server = null;

  public HiccWebServer() {
    DaemonWatcher.createInstance("hicc");
    serverConf = HiccWebServer.class.getResource("/WEB-INF/jetty.xml");
    if(serverConf==null) {
      log.error("Unable to locate jetty-web.xml.");
      DaemonWatcher.bailout(-1);
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
    HiccWebServer hicc = new HiccWebServer();
    hicc.run();
  }
}
