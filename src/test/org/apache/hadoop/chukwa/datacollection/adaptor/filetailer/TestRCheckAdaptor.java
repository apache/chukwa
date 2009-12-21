package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;
import org.apache.hadoop.conf.Configuration;
import junit.framework.TestCase;
import org.apache.hadoop.chukwa.datacollection.adaptor.TestDirTailingAdaptor;
import org.apache.log4j.Level;

public class TestRCheckAdaptor extends TestCase {
  
  ChunkCatcherConnector chunks;

  public TestRCheckAdaptor() {
    chunks = new ChunkCatcherConnector();
    chunks.start();
  }

  public void testLogRotate() throws IOException, InterruptedException,
      ChukwaAgent.AlreadyRunningException {
    Configuration conf = new Configuration();
    conf.set("chukwaAgent.control.port", "0");
    conf.setInt("chukwaAgent.adaptor.context.switch.time", 100);
        
//    RCheckFTAdaptor.log.setLevel(Level.DEBUG);
    File baseDir = new File(System.getProperty("test.build.data", "/tmp") + "/rcheck");
    TestDirTailingAdaptor.createEmptyDir(baseDir);
    File tmpOutput = new File(baseDir, "rotateTest.1");
    PrintWriter pw = new PrintWriter(new FileOutputStream(tmpOutput));
    pw.println("First");
    pw.close();
    Thread.sleep(1000);//to make sure mod dates are distinguishing.
    tmpOutput = new File(baseDir, "rotateTest");
    pw = new PrintWriter(new FileOutputStream(tmpOutput));
    pw.println("Second");
    pw.close();
    
    
    ChukwaAgent agent = new ChukwaAgent(conf);
    String adaptorID = agent.processAddCommand("add lr = filetailer.RCheckFTAdaptor test " + tmpOutput.getAbsolutePath() + " 0");
    assertNotNull(adaptorID);
    
    Chunk c = chunks.waitForAChunk(2000);
    assertNotNull(c);
    assertTrue(c.getData().length == 6);
    assertTrue("First\n".equals(new String(c.getData())));
    c = chunks.waitForAChunk(2000);
    assertNotNull(c);
    assertTrue(c.getData().length == 7);    
    assertTrue("Second\n".equals(new String(c.getData())));

    pw = new PrintWriter(new FileOutputStream(tmpOutput, true));
    pw.println("Third");
    pw.close();
    c = chunks.waitForAChunk(2000);
    
    assertNotNull(c);
    assertTrue(c.getData().length == 6);    
    assertTrue("Third\n".equals(new String(c.getData())));
    Thread.sleep(1500);
    
    tmpOutput.renameTo(new File(baseDir, "rotateTest.2"));
    pw = new PrintWriter(new FileOutputStream(tmpOutput, true));
    pw.println("Fourth");
    pw.close();
    c = chunks.waitForAChunk(2000);

    assertNotNull(c);
    System.out.println("got " + new String(c.getData()));
    assertTrue("Fourth\n".equals(new String(c.getData())));

    Thread.sleep(1500);
    
    tmpOutput.renameTo(new File(baseDir, "rotateTest.3"));
    Thread.sleep(400);
    pw = new PrintWriter(new FileOutputStream(tmpOutput, true));
    pw.println("Fifth");
    pw.close();
    c = chunks.waitForAChunk(2000);
    assertNotNull(c);
    System.out.println("got " + new String(c.getData()));
    assertTrue("Fifth\n".equals(new String(c.getData())));

    agent.shutdown();
    Thread.sleep(2000);
  }

}
