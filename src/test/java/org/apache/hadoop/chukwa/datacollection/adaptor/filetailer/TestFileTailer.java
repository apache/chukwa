package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;

import static org.apache.hadoop.chukwa.util.TempFileUtil.makeTestFile;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent.AlreadyRunningException;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;
import org.junit.After;
import org.junit.Test;

public class TestFileTailer {
	private ChukwaAgent agent;
	private String adaptorId;
	private File testFile;

	@After
	public void tearDown() throws Exception {
		agent.stopAdaptor(adaptorId, false);
		agent.shutdown();
		if (testFile != null) {
			testFile.delete();
		}
	}

	@Test
	public void testDontSleepIfHasMoreData() throws AlreadyRunningException, IOException, InterruptedException {
		ChukwaConfiguration cc = new ChukwaConfiguration();
		cc.setInt("chukwaAgent.fileTailingAdaptor.maxReadSize", 18); // small in order to have hasMoreData=true
																	 // (with 26 letters we should have 2 chunks)
		agent = new ChukwaAgent(cc);
		
		ChunkCatcherConnector chunks = new ChunkCatcherConnector();
	    chunks.start();

	    File baseDir = new File(System.getProperty("test.build.data", "/tmp"));
		testFile = makeTestFile("testDontSleepIfHasMoreData", 1, baseDir); // insert 26 letters on file
		long startTime = System.currentTimeMillis();
		adaptorId = agent.processAddCommand("add adaptor_test ="
				+ "filetailer.FileTailingAdaptor testDontSleepIfHasMoreData "
				+ testFile.getCanonicalPath() + " 0");

		chunks.waitForAChunk();
		chunks.waitForAChunk();
		
		long endTime = System.currentTimeMillis();
		assertTrue( endTime - startTime < 300 ); // ensure that everything finishes very fast
												 // faster than SAMPLE_PERIOD_MS (ie: we don't sleep)
	}

}
