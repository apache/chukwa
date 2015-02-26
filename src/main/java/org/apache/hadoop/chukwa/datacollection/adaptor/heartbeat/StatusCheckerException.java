package org.apache.hadoop.chukwa.datacollection.adaptor.heartbeat;

public class StatusCheckerException extends Exception {
  
  private static final long serialVersionUID = -1039172824878846049L;

  public StatusCheckerException() {
    super();
  }

  public StatusCheckerException(String arg0, Throwable arg1) {
    super(arg0, arg1);
  }

  public StatusCheckerException(String arg0) {
    super(arg0);
  }

  public StatusCheckerException(Throwable arg0) {
    super(arg0);
  }
}
