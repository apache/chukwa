package org.apache.hadoop.chukwa.caffe;

import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.chukwa.datastore.ChukwaHBaseStore;
import org.apache.hadoop.chukwa.hicc.bean.Series;
import org.json.simple.JSONObject;

//export CLASSPATH=/opt/apache/hadoop/etc/hadoop:/opt/apache/hbase/conf:/opt/apache/chukwa-0.8.0/share/chukwa/*:/opt/apache/chukwa-0.8.0/share/chukwa/lib/*:$CLASSPATH

public class MetricsCollector
{
  private Timer getMetricSnapshotTimer = null;
  private long intervalInMilli;
  private String hostname;
  private String dirName;
  
  public MetricsCollector (long intervalInMilli, String hostname, String dirName) {
    this.intervalInMilli = intervalInMilli;
    this.hostname = hostname;
    this.dirName = dirName;
    getMetricSnapshotTimer = new Timer ("GetMetricSnapshot", true);
  }
  
  public void start () {
    if (getMetricSnapshotTimer != null)
      getMetricSnapshotTimer.schedule (new GetMetricSnapshotTimerTask (hostname, intervalInMilli, dirName), 0, intervalInMilli);    
  }
  
  public void cancel ()
  {
    if (getMetricSnapshotTimer != null)
      getMetricSnapshotTimer.cancel ();
  }
  
  class GetMetricSnapshotTimerTask extends TimerTask
  {
    private String hostname = null;
    private BufferedWriter bufferedWriter = null;
    private long intervalInMilli;
    private String dirName;

    /**
     * Normalize the timestamp in time series data to use seconds
     */
    private final static int XSCALE = 1000;

    GetMetricSnapshotTimerTask (String hostname, long intervalInMilli, String dirName)
    {
      this.hostname = hostname;
      this.intervalInMilli = intervalInMilli;
      this.dirName = dirName;
    }

    public void run () 
    {
      TimeZone tz = TimeZone.getTimeZone("UTC");
      Calendar now = Calendar.getInstance(tz);
      long currTime=now.getTimeInMillis();

      System.out.println ("currTime in UTC: " + currTime);
      System.out.println ("currTime in current time zone" + System.currentTimeMillis ());

      long startTime = currTime - intervalInMilli;
      long endTime = currTime;
      try {
        System.out.println ("About to run");
        getHadoopMetrics (startTime, endTime);
        System.out.println ("Done run");
      } catch (Exception e) {
        e.printStackTrace ();
      }
    }

    private void getHadoopMetrics(long startTime, long endTime) throws Exception 
    {
      String source = hostname + ":NodeManager";
      System.out.println ("source: " + source);
      System.out.println ("startTime: " + startTime);
      System.out.println ("endTime: " + endTime); 
      Series series = ChukwaHBaseStore.getSeries ("HadoopMetrics.jvm.JvmMetrics.MemHeapUsedM", source, startTime, endTime);
      String value = series.toString ();
      System.out.println ("value: " + value);

      JSONObject jsonObj = (JSONObject) series.toJSONObject ();
      Set set = jsonObj.keySet ();
      Iterator iter = set.iterator ();
      List list = (List) jsonObj.get ("data");
      if (list != null) { 
        int size = list.size ();
        System.out.println ("size: " + size);
        if (size > 0 ) {
          String name = "NodeManager" + "_" + "HadoopMetrics.jvm.JvmMetrics.MemHeapUsedM" + "_" + hostname;
          generateCsv (list, name, startTime, bufferedWriter);
        }
      }
    }

    private void generateCsv (List list, String name, long startTime, BufferedWriter bufferedWriter) throws Exception
    {
      String fileName = dirName + "/" + name + "_" + startTime;
      PrintWriter writer = new PrintWriter(fileName + ".csv", "UTF-8");
      int size = list.size ();
      for (int i = 0; i < size; i++) {
        List point = (List) list.get (i);
        long time = (Long) point.get (0) / XSCALE;
        double val = (Double) point.get (1);
        writer.println(time + "," + val);
      }
      writer.close();
    }
  }
}
