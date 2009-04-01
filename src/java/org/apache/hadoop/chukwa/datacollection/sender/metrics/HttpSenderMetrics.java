package org.apache.hadoop.chukwa.datacollection.sender.metrics;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;

public class HttpSenderMetrics implements Updater {

  public MetricsRegistry registry = new MetricsRegistry();
  private MetricsRecord metricsRecord;
  private HttpSenderActivityMBean mbean;
  
  
  public MetricsTimeVaryingInt collectorRollover =
    new MetricsTimeVaryingInt("collectorRollover", registry,"number of collector rollovert");
  
  public MetricsTimeVaryingInt httpPost =
    new MetricsTimeVaryingInt("httpPost", registry,"number of HTTP post");
  
  public MetricsTimeVaryingInt httpException =
    new MetricsTimeVaryingInt("httpException", registry,"number of HTTP Exception");

  public MetricsTimeVaryingInt httpThrowable =
    new MetricsTimeVaryingInt("httpThrowable", registry,"number of HTTP Throwable exception");
  
  public MetricsTimeVaryingInt httpTimeOutException =
    new MetricsTimeVaryingInt("httpTimeOutException", registry,"number of HTTP TimeOutException");
  
  /** Creates a new instance of HttpSenderMetrics */
  public HttpSenderMetrics(String processName, String recordName) {
      MetricsContext context = MetricsUtil.getContext(recordName);
      metricsRecord = MetricsUtil.createRecord(context, recordName);
      metricsRecord.setTag("process", processName);
      mbean = new HttpSenderActivityMBean(registry, recordName);
      context.registerUpdater(this);
  }


  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }

  public void shutdown() {
    if (mbean != null)
      mbean.shutdown();
  }

}
