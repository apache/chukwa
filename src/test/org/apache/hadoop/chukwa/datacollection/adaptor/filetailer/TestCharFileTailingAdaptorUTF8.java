package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import junit.framework.TestCase;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;
import org.apache.hadoop.conf.Configuration;

public class TestCharFileTailingAdaptorUTF8 extends TestCase {
  ChunkCatcherConnector chunks;

  public TestCharFileTailingAdaptorUTF8() {
    chunks = new ChunkCatcherConnector();
    chunks.start();
  }

  public void testCrSepAdaptor() throws IOException, InterruptedException,
      ChukwaAgent.AlreadyRunningException {
    
    Configuration conf = new Configuration();
    conf.set("chukwaAgent.control.port", "0");
    ChukwaAgent agent = new ChukwaAgent(conf);
    File testFile = makeTestFile("chukwaTest", 80);
    String adaptorId = agent
        .processAddCommand("add test = org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8"
            + " lines " + testFile + " 0");
    assertTrue(adaptorId.equals("test"));
    System.out.println("getting a chunk...");
    Chunk c = chunks.waitForAChunk();
    assertTrue(c.getSeqID() == testFile.length());

    assertTrue(c.getRecordOffsets().length == 80);
    int recStart = 0;
    for (int rec = 0; rec < c.getRecordOffsets().length; ++rec) {
      String record = new String(c.getData(), recStart,
          c.getRecordOffsets()[rec] - recStart + 1);
      assertTrue(record.equals(rec + " abcdefghijklmnopqrstuvwxyz\n"));
      recStart = c.getRecordOffsets()[rec] + 1;
    }
    assertTrue(c.getDataType().equals("lines"));
    agent.stopAdaptor(adaptorId, false);
    agent.shutdown();
    Thread.sleep(2000);
  }

  private File makeTestFile(String name, int size) throws IOException {
    File tmpOutput = new File(System.getProperty("test.build.data", "/tmp"),
        name);
    FileOutputStream fos = new FileOutputStream(tmpOutput);

    PrintWriter pw = new PrintWriter(fos);
    for (int i = 0; i < size; ++i) {
      pw.print(i + " ");
      pw.println("abcdefghijklmnopqrstuvwxyz");
    }
    pw.flush();
    pw.close();
    return tmpOutput;
  }

}
