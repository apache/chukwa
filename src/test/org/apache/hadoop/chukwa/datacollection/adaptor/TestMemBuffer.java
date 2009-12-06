package org.apache.hadoop.chukwa.datacollection.adaptor;

import static org.apache.hadoop.chukwa.util.TempFileUtil.makeTestFile;
import java.io.File;
import java.io.IOException;
import junit.framework.TestCase;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;
import org.apache.hadoop.conf.Configuration;

public class TestMemBuffer extends TestCase {

  Configuration conf = new Configuration();
  static File baseDir;
  File testFile;
  ChunkCatcherConnector chunks;
  
  public TestMemBuffer() throws IOException {
    baseDir = new File(System.getProperty("test.build.data", "/tmp"));
    conf.setInt("chukwaAgent.control.port", 0);
    conf.set("chukwaAgent.checkpoint.dir", baseDir.getCanonicalPath());
    conf.setBoolean("chukwaAgent.checkpoint.enabled", false);
    conf.setInt("chukwaAgent.adaptor.fileadaptor.timeoutperiod", 100);
    conf.setInt("chukwaAgent.adaptor.context.switch.time", 100);
    testFile = makeTestFile(baseDir);

    chunks = new ChunkCatcherConnector();
    chunks.start();
  }
  
  
  //start a wrapped FileAdaptor. Pushes a chunk. Stop it and restart.
  //chunk hasn't been acked, so should get pushed again.
  //we delete the file and also change the data type each time through the loop
  //to make sure we get the cached chunk.
  public void testResendAfterStop()  throws IOException,
  ChukwaAgent.AlreadyRunningException, InterruptedException {
    
    ChukwaAgent agent = new ChukwaAgent(conf);
    
    assertEquals(0, agent.adaptorCount());

    for(int i=0; i< 5; ++i) {
      String name =agent.processAddCommand("add adaptor_test = MemBuffered FileAdaptor raw"+i+ " "+testFile.getCanonicalPath() + " 0");
      assertEquals(name, "adaptor_test");
      Chunk c = chunks.waitForAChunk(5000);
      assertNotNull(c);
      String dat = new String(c.getData());
      assertTrue(dat.startsWith("0 abcdefghijklmnopqrstuvwxyz"));
      assertTrue(dat.endsWith("9 abcdefghijklmnopqrstuvwxyz\n"));
      assertTrue(c.getDataType().equals("raw0"));
      agent.stopAdaptor(name, true);
      testFile.delete(); //file won't be there after the first time around.
    }    
    assertEquals(0, agent.adaptorCount());

    agent.shutdown();
  }

}
