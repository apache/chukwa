package org.apache.hadoop.chukwa.util;

public class DaemonWatcher extends PidFile {
  private static DaemonWatcher instance = null;
  
  public synchronized static DaemonWatcher createInstance(String name) {
    if(instance == null) {
      instance = new DaemonWatcher(name);
      Runtime.getRuntime().addShutdownHook(instance);
    }
    return instance;
  }
  
  public static DaemonWatcher getInstance() {
    return instance;
  }
  
  private DaemonWatcher(String name) {
    super(name);
  }
  
  public static void bailout(int status) {
    if(instance != null) {
      Runtime.getRuntime().removeShutdownHook(instance);
    }
    System.exit(status);
  }
}
