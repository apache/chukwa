package org.apache.hadoop.chukwa.util;

import org.apache.log4j.Logger;
import org.apache.log4j.nagios.Nsca;

public class NagiosHelper {
  static Logger log = Logger.getLogger(NagiosHelper.class);
  public static final int NAGIOS_OK       = Nsca.NAGIOS_OK;
  public static final int NAGIOS_WARN     = Nsca.NAGIOS_WARN;
  public static final int NAGIOS_CRITICAL = Nsca.NAGIOS_CRITICAL;
  public static final int NAGIOS_UNKNOWN  = Nsca.NAGIOS_UNKNOWN;
  
  public static void sendNsca(String nagiosHost,int nagiosPort,String reportingHost,String reportingService,String msg,int state) {
    Nsca nsca = new Nsca();
    try {
      nsca.send_nsca(nagiosHost, ""+nagiosPort, reportingHost, reportingService, msg, state, 1);
    } catch (Throwable e) {
     log.warn(e);
    }
     
  }
}
