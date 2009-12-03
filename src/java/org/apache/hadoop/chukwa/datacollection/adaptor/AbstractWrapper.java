package org.apache.hadoop.chukwa.datacollection.adaptor;

import java.util.*;
import java.util.regex.*;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorFactory;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorManager;

public class AbstractWrapper implements NotifyOnCommitAdaptor,ChunkReceiver {
 
  Adaptor inner;
  String innerClassName;
  String innerType;
  ChunkReceiver dest;

  @Override
  public String getCurrentStatus() {
    return innerClassName + " " + inner.getCurrentStatus();
  }

  @Override
  public void hardStop() throws AdaptorException {
    inner.hardStop();
  }

  static Pattern p = Pattern.compile("([^ ]+) +([^ ].*)");
  
  /**
   * Note that the name of the inner class will get parsed out as a type
   */
  @Override
  public String parseArgs(String innerClassName, String params) {
    Matcher m = p.matcher(params);
    this.innerClassName = innerClassName;
    String innerCoreParams;
    if(m.matches()) {
      innerType = m.group(1);
      inner = AdaptorFactory.createAdaptor(innerClassName);
      innerCoreParams = inner.parseArgs(innerType,m.group(2));
      return innerClassName + innerCoreParams;
    }
    else return null;
  }

  @Override
  public long shutdown() throws AdaptorException {
    return inner.shutdown();
  }

  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy)
      throws AdaptorException {
    return inner.shutdown(shutdownPolicy);
  }

  @Override
  public String getType() {
    return innerType;
  }

  /**
   * Note that the name of the inner class will get parsed out as a type
   */
  @Override
  public void start(String adaptorID, String type, long offset,
      ChunkReceiver dest, AdaptorManager c) throws AdaptorException {
    String dummyAdaptorID = adaptorID;
    this.dest = dest;
    inner.start(dummyAdaptorID, type, offset, this, c);
  }

  @Override
  public void add(Chunk event) throws InterruptedException {
    dest.add(event);
  }

  @Override
  public void committed(long commitedByte) { }

}
