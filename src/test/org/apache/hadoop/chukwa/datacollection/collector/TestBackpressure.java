package org.apache.hadoop.chukwa.datacollection.collector;

import junit.framework.TestCase;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.ServletCollector;
import org.apache.hadoop.chukwa.datacollection.connector.http.HttpConnector;
import org.apache.hadoop.chukwa.datacollection.writer.NullWriter;
import org.apache.hadoop.chukwa.datacollection.writer.PipelineStageWriter;
import org.apache.hadoop.chukwa.util.ConstRateAdaptor;
import org.apache.hadoop.conf.Configuration;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public class TestBackpressure extends TestCase {

  int PORTNO = 9991;
  int SLEEP_SEC = 20;
  int WRITE_RATE = 300; //kb/sec
  int SEND_RATE = 2500000; //bytes/sec
  
  public void testBackpressure() throws Exception {
    Configuration conf = new Configuration();
    conf.set("chukwaCollector.writerClass", NullWriter.class
        .getCanonicalName());
    conf.set(NullWriter.RATE_OPT_NAME, ""+WRITE_RATE);//kb/sec
    conf.setInt(HttpConnector.MIN_POST_INTERVAL_OPT, 100);
    conf.setInt("constAdaptor.sleepVariance", 1);
    conf.setInt("constAdaptor.minSleep", 50);
    
    conf.setInt("chukwaAgent.control.port", 0);
    ChukwaAgent agent = new ChukwaAgent(conf);
    HttpConnector conn = new HttpConnector(agent, "http://localhost:"+PORTNO+"/chukwa");
    conn.start();
    Server server = new Server(PORTNO);
    Context root = new Context(server, "/", Context.SESSIONS);

    root.addServlet(new ServletHolder(new ServletCollector(conf)), "/*");
    server.start();
    server.setStopAtShutdown(false);
    Thread.sleep(1000);
    agent.processAddCommand("add constSend = " + ConstRateAdaptor.class.getCanonicalName() + 
        " testData "+ SEND_RATE + " 0");
    Thread.sleep(SLEEP_SEC * 1000);

    String stat = agent.getAdaptorList().get("constSend");
    long bytesPerSec = Long.valueOf(stat.split(" ")[3]) / SLEEP_SEC / 1000;
    System.out.println("data rate was " + bytesPerSec + " kb /second");
    assertTrue(bytesPerSec < WRITE_RATE);
    assertTrue(bytesPerSec > 3* WRITE_RATE / 4);//an assumption, but should hold true
    agent.shutdown();
  }
}
