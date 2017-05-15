package chukwa;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Calendar;
import java.util.*;

import javax.imageio.ImageIO;

import org.json.simple.JSONObject;
import org.apache.hadoop.chukwa.datastore.ChukwaHBaseStore;
import org.apache.hadoop.chukwa.hicc.bean.Series;

// http://10.177.68.181:8080/job/Reinstall%20Cluster/api/json
// curl -X GET -u root:password -i http://10.177.68.181:8080/job/Create%20Cluster/147/parameters/

// hbase(main):011:0> get 'chukwa_meta', "HBaseMetrics"


//yum -y install java-1.8.0-openjdk-devel.x86_64

//export CLASSPATH=/opt/apache/hadoop/etc/hadoop:/opt/apache/hbase/conf:/opt/apache/chukwa-0.8.0/share/chukwa/*:/opt/apache/chukwa-0.8.0/share/chukwa/lib/*:$CLASSPATH

//export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/apache/hadoop/bin:/opt/apache/hbase/bin
//su hdfs -c "hadoop dfs -mkdir -p /user/hdfs"
//while :
//do
//  su hdfs -c "hadoop jar /opt/apache/hadoop-2.7.2/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.2.jar teragen 100 /user/hdfs/terasort-input"
//  su hdfs -c "hadoop jar /opt/apache/hadoop-2.7.2/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.2.jar terasort /user/hdfs/terasort-input /user/hdfs/terasort-output"
//  su hdfs -c "hadoop dfs -rmr  -skipTrash /user/hdfs/terasort-input/"
//  su hdfs -c "hadoop dfs -rmr  -skipTrash /user/hdfs/terasort-output/"
//done

public class MetricsCollector
{
  final static String[] system_metrics = { 
    "SystemMetrics.LoadAverage.1",
    "SystemMetrics.cpu.combined", 
    "SystemMetrics.cpu.sys", 
    "SystemMetrics.cpu.user",
    "SystemMetrics.memory.FreePercent", 
    "SystemMetrics.memory.UsedPercent",
    "SystemMetrics.disk.ReadBytes", 
    "SystemMetrics.disk.WriteBytes",
    "SystemMetrics.network.TxBytes", 
    "SystemMetrics.network.RxBytes",
    "SystemMetrics.swap.Total", 
    "SystemMetrics.swap.Used", 
    "SystemMetrics.swap.Free"
  };

  final static String [] hadoop_metrics = {
    "HadoopMetrics.jvm.JvmMetrics.MemHeapUsedM", 
    //"HadoopMetrics.jvm.JvmMetrics.MemHeapMaxM",
    //"HadoopMetrics.dfs.FSNamesystem.CapacityRemainingGB", 
    //"HadoopMetrics.dfs.FSNamesystem.CapacityTotalGB",
    //"HadoopMetrics.yarn.ClusterMetrics.NumActiveNMs", 
    //"HadoopMetrics.yarn.ClusterMetrics.NumLostNMs",
    //"HadoopMetrics.dfs.FSNamesystem.HAState" ,
    //"HadoopMetrics.dfs.FSNamesystem.TotalLoad",
    //"HadoopMetrics.rpc.rpc.RpcProcessingTimeAvgTime",
    //"HadoopMetrics.dfs.FSNamesystem.StaleDataNodes"
  };
  //final static String [] hadoop_processes = {"NameNode", "DataNode", "NodeManager", "ResourceManager"};
  final static String [] hadoop_processes = {"NodeManager"};

  final static String [] hbase_metrics = {
    "HBaseMetrics.jvm.JvmMetrics.MemHeapUsedM", 
    //"HBaseMetrics.jvm.JvmMetrics.MemHeapMaxM"
  };
  final static String [] hbase_processes = {"Master", "RegionServer"}; 

  private Timer getMetricSnapshotTimer = null;
  private String hostname = null;
  private int intervalInMin;
  private long intervalInMilli;
  
  MetricsCollector (String hostname, int intervalInMin) 
  {
    this.hostname = hostname;
    this.intervalInMin = intervalInMin;
    this.intervalInMilli = intervalInMin * 60 * 1000;
  }
  
  public void startGetMetricSnapshotTimer (MetricsCollector tester)
  {
    getMetricSnapshotTimer = new Timer ("GetMetricSnapshot", true);
    getMetricSnapshotTimer.schedule (new GetMetricSnapshotTimerTask (tester), 0, intervalInMilli);
  }
  
  public void cancelFetMetricSnapshotTimer ()
  {
    if (getMetricSnapshotTimer!= null)
      getMetricSnapshotTimer.cancel ();
  }
   
  class GetMetricSnapshotTimerTask extends TimerTask
  {
    MetricsCollector tester = null;
    String hostname = null;
    BufferedWriter bufferedWriter = null;
    
    GetMetricSnapshotTimerTask (MetricsCollector tester)
    {
      this.tester = tester;
      hostname = tester.hostname;
      String outputFileName = "labels.txt";

      try {
        FileWriter fileWriter = new FileWriter(outputFileName);
        bufferedWriter = new BufferedWriter(fileWriter);
      } catch (IOException e) {
        e.printStackTrace ();
      }

    }
    
    public void run () 
    {
      // one hour
      TimeZone tz = TimeZone.getTimeZone("UTC");
      Calendar now = Calendar.getInstance(tz);
      long currTime=now.getTimeInMillis();

      System.out.println ("currTime in UTC: " + currTime);
      System.out.println ("currTime in current time zone" + System.currentTimeMillis ());

      long startTime = currTime - intervalInMilli;
      long endTime = currTime;
      try {
        System.out.println ("About to run");
        //tester.getSystemMetrics (hostname, startTime, endTime);
        //tester.getHbaseMetrics (hostname, startTime, endTime);
        tester.getHadoopMetrics (hostname, startTime, endTime, bufferedWriter);
        System.out.println ("Done run");
      } catch (Exception e) {
        e.printStackTrace ();
      }
    }
  }

  
  private void getHadoopMetrics (String hostname, long startTime, long endTime, BufferedWriter bufferedWriter) throws Exception
  {

    for (int j = 0; j < hadoop_metrics.length; j++) {
      System.out.println ("--------------------------------");
      System.out.println ("metrics: " + hadoop_metrics [j]);
      for (int i = 0; i < hadoop_processes.length; i++) {
        String source = hostname + ":" + hadoop_processes [i];
        System.out.println ("source: " + source);
        System.out.println ("startTime: " + startTime);
        System.out.println ("endTime: " + endTime); 
        Series series = ChukwaHBaseStore.getSeries (hadoop_metrics [j], source, startTime, endTime);
        String value = series.toString ();
        System.out.println ("value: " + value);

        JSONObject jsonObj = (JSONObject) series.toJSONObject ();
        Set set = jsonObj.keySet ();
        Iterator iter = set.iterator ();
        List list = (List) jsonObj.get ("data");
        if (list == null) 
          continue;
        int size = list.size ();
        System.out.println ("size: " + size);

        if (size > 0 ) {
          String name = hadoop_metrics [j] + "_" + hadoop_processes [i] + "_" + hostname;
          drawImage (list, name, startTime, endTime, bufferedWriter);
        }
      }
    }
  }

  private void getHbaseMetrics (String hostname, long startTime, long endTime, BufferedWriter bufferedWriter) throws Exception
  {
    for (int j = 0; j < hbase_metrics.length; j++) {
      System.out.println ("--------------------------------");
      System.out.println ("metrics: " + hbase_metrics [j]);
      for (int i = 0; i < hbase_processes.length; i++) {
        String source = hostname + ":" + hbase_processes [i];
        System.out.println ("source: " + source);
        System.out.println ("startTime: " + startTime);
        System.out.println ("endTime: " + endTime); 
        Series series = ChukwaHBaseStore.getSeries (hbase_metrics [j], source, startTime, endTime);
        String value = series.toString ();
        System.out.println ("value: " + value);

        JSONObject jsonObj = (JSONObject) series.toJSONObject ();
        Set set = jsonObj.keySet ();
        Iterator iter = set.iterator ();
        List list = (List) jsonObj.get ("data");
        if (list == null) 
          continue;
        int size = list.size ();
        System.out.println ("size: " + size);

        if (size > 0 ) {
          String name = hbase_metrics [j] + "_" + hbase_processes [i] + "_" + hostname;
          drawImage (list, name, startTime, endTime, bufferedWriter);
        }
      }
    }
  }  

  private void getSystemMetrics (String source, long startTime, long endTime, BufferedWriter bufferedWriter) throws Exception
  {
    for (int j = 0; j < system_metrics.length; j++) {
      System.out.println ("--------------------------------");

      System.out.println ("metrics: " + system_metrics [j]);
      System.out.println ("source: " + source);
      System.out.println ("startTime: " + startTime);
      System.out.println ("endTime: " + endTime); 
      Series series = ChukwaHBaseStore.getSeries (system_metrics [j], source, startTime, endTime);
      String value = series.toString ();
      System.out.println ("value: " + value);

      JSONObject jsonObj = (JSONObject) series.toJSONObject ();
      Set set = jsonObj.keySet ();
      Iterator iter = set.iterator ();
      List list = (List) jsonObj.get ("data");
      if (list == null) 
        continue;
      int size = list.size ();
      System.out.println ("size: " + size);

      if (size > 0 ) {
        String name = system_metrics [j]  + "_" + hostname;
        drawImage (list, name, startTime, endTime, bufferedWriter);
      }
    }
  }

  public void drawImage (List list, String name, long startTime, long endTime, BufferedWriter bufferedWriter) throws Exception
  {
    String fileName = name + "_" + startTime;
    PrintWriter writer = new PrintWriter(fileName + ".csv", "UTF-8");
    int size = list.size ();
    long startTimeX = (Long) ((List) list.get (0)).get (0) / MyPoint.XSCALE;
    long endTimeX = (Long) ((List) list.get (size - 1 )).get (0) / MyPoint.XSCALE;
    int x_size = (int) (endTimeX - startTimeX);
    int y_size = 1000;
    System.out.println ("x_size: " + x_size);
    BufferedImage img = new BufferedImage(x_size, y_size, BufferedImage.TYPE_INT_ARGB);

    Graphics2D ig2 = img.createGraphics();
    ig2.setBackground(Color.WHITE);

    ig2.setColor (Color.BLUE);
    ig2.setStroke(new BasicStroke(2));

    MyPoint prevPoint = null;
    MyPoint currPoint = null;
    for (int i = 0; i < size; i++) {
      List point = (List) list.get (i);
      long time = (Long) point.get (0);
      double val = (Double) point.get (1);
      currPoint = new MyPoint (time, val);
      System.out.println ("time:" + time + ", value:" + val);

      if (prevPoint != null) {
        int x1 = (int) (prevPoint.time - startTimeX);
        int x2 = (int) (currPoint.time - startTimeX);
        
        int y1 = (int) (y_size - prevPoint.data);
        int y2 = (int) (y_size - currPoint.data);
        
        System.out.println ("point 1: " + x1 + ", " + y1);
        System.out.println ("point 2: " + x2 + ", " + y2);
        
        ig2.drawLine ((int) (prevPoint.time - startTimeX), (int) (y_size - prevPoint.data), (int) (currPoint.time - startTimeX), (int) (y_size - currPoint.data));
      }  
      prevPoint = currPoint;
      writer.println(currPoint.time + "," + currPoint.data );
      
    }
    String imageFileName = fileName + ".png"; 
    File f = new File(imageFileName);
    ImageIO.write(img, "PNG", f);
    bufferedWriter.write (imageFileName + " 0\n");
    bufferedWriter.flush ();
    writer.close();
  }
  

  static class MyPoint 
  {
    public long time;
    public double data;
    static int XSCALE = 1000;
    static int YSCALE = 100;    

    public MyPoint (long time, double data) {
      this.time = time/XSCALE;
      this.data = data;
    }
  }

  public static void main (String [] args) throws Exception {
    String hostname = args [0];
    String interval = args [1]; // in min
    int intervalInMin = Integer.parseInt (interval);

    MetricsCollector tester = new MetricsCollector (hostname, intervalInMin);
    tester.startGetMetricSnapshotTimer (tester);
   
    try {
      Object lock = new Object();
      synchronized (lock) {
        while (true) {
          lock.wait();
        }
      }
    } catch (InterruptedException ex) {
    }
  }


}
