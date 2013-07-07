package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;

import static org.apache.hadoop.chukwa.util.TempFileUtil.makeTestFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFileTailingAdaptorPreserveLines {
  private static File testFile;
  // private static String adaptorToTest = "FileTailingAdaptor";
  // private static String adaptorToTest = "CharFileTailingAdaptorUTF8";
  private static String adaptorToTest = "FileTailingAdaptorPreserveLines";
  private static ChukwaConfiguration conf;
  private ChukwaAgent agent;
  private String adaptorId;
  private ChunkCatcherConnector chunks;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    File baseDir = new File(System.getProperty("test.build.data", "/tmp"));
    testFile = makeTestFile("TestFileTailingAdaptorPreserveLines", 10,
        baseDir);

    conf = new ChukwaConfiguration();
    conf.setInt("chukwaAgent.fileTailingAdaptor.maxReadSize", 130);
  }

  /**
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    agent = new ChukwaAgent(conf);
    chunks = new ChunkCatcherConnector();
    chunks.start();

    adaptorId = agent.processAddCommand("add adaptor_test =" + "filetailer."
        + adaptorToTest + " TestFileTailingAdaptorPreserveLines "
        + testFile.getCanonicalPath() + " 0");
  }

  @After
  public void tearDown() throws Exception {
    agent.stopAdaptor(adaptorId, false);
    agent.shutdown();
    chunks.clear();
    chunks.shutdown();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (testFile != null) {
      testFile.delete();
    }
  }

  /**
   * Check that chunk does not break lines (which is the problem of
   * FileTailingAdaptor adaptor)
   * 
   * @throws UnsupportedEncodingException
   */
  @Test
  public void testDontBreakLines() throws UnsupportedEncodingException {
    Chunk c = chunks.waitForAChunk(5000);
    String data = new String(c.getData(), "UTF-8");

    String[] lines = data.split("\\r?\\n");

    // Check that length of the last line is the same as the
    // one of the first line. Otherwise, it means the last
    // line has been cut
    assertEquals(lines[0].length(), lines[lines.length - 1].length());
  }

  /**
   * Check that second chunk contains the data that just follow the first
   * chunk's data
   * 
   * @throws UnsupportedEncodingException
   */
  @Test
  public void testSecondChunkDataFollowsFirstChunkData()
      throws UnsupportedEncodingException {
    Chunk c = chunks.waitForAChunk(5000);
    String data = new String(c.getData(), "UTF-8");
    String[] lines1 = data.split("\\r?\\n");

    c = chunks.waitForAChunk(5000);
    data = new String(c.getData(), "UTF-8");
    String[] lines2 = data.split("\\r?\\n");

    int numLastLineChunk1 = (int) (lines1[lines1.length - 1].charAt(0));
    int numLastLineChunk2 = (int) (lines2[0].charAt(0));

    // Check that lines numbers are successive between
    // last line of first chunk and first line of second chunk
    assertEquals(numLastLineChunk1, numLastLineChunk2 - 1);
  }

  /**
   * Check that chunk only has one set record offset although it has more than 2
   * lines (which is the contrary of CharFileTailingAdaptorUTF8)
   * 
   * @throws UnsupportedEncodingException
   */
  @Test
  public void testOnlyOneSetRecordOffset()
      throws UnsupportedEncodingException {
    Chunk c = chunks.waitForAChunk(5000);
    String data = new String(c.getData(), "UTF-8");
    String[] lines = data.split("\\r?\\n");

    // Check that we have more than two lines
    assertTrue(lines.length > 2);

    int[] offsets_i = c.getRecordOffsets();

    // Check that we only have one offset
    assertEquals(1, offsets_i.length);
  }
}
