package org.apache.hadoop.chukwa.datacollection.writer;

import java.util.List;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.conf.Configuration;

/**
 * Minimal writer; does nothing with data.
 * 
 * Useful primarily as an end-of-pipeline stage, if stuff in the middle
 * is accomplishing something useful.
 *
 */
public class NullWriter implements ChukwaWriter {
  
  //in kb per sec
  int maxDataRate = Integer.MAX_VALUE;
  public static final String RATE_OPT_NAME = "nullWriter.dataRate";
  @Override
  public void add(List<Chunk> chunks) throws WriterException {
    try {
      int dataBytes =0;
      for(Chunk c: chunks)
        dataBytes +=c.getData().length;
      if(maxDataRate > 0)
        Thread.sleep(dataBytes / maxDataRate);
    } catch(Exception e) {}
    return;
  }

  @Override
  public void close() throws WriterException {
    return;
  }

  @Override
  public void init(Configuration c) throws WriterException {
    maxDataRate = c.getInt(RATE_OPT_NAME, 0);
    return;
  }

}
