package org.apache.hadoop.chukwa.datacollection.agent.metrics;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;

public class AgentMetrics implements Updater {
  public static final AgentMetrics agentMetrics = new AgentMetrics("ChukwaAgent", "chukwaAgent");;
  
  public MetricsRegistry registry = new MetricsRegistry();
  private MetricsRecord metricsRecord;
  private AgentActivityMBean agentActivityMBean;

  public MetricsIntValue adaptorCount =
    new MetricsIntValue("adaptorCount", registry,"number of new adaptor");

  public MetricsTimeVaryingInt addedAdaptor =
    new MetricsTimeVaryingInt("addedAdaptor", registry,"number of added adaptor");
  
  public MetricsTimeVaryingInt removedAdaptor =
    new MetricsTimeVaryingInt("removedAdaptor", registry,"number of removed adaptor");
  
  /** Creates a new instance of AgentMetrics */
  public AgentMetrics(String processName, String recordName) {
      MetricsContext context = MetricsUtil.getContext(recordName);
      metricsRecord = MetricsUtil.createRecord(context, recordName);
      metricsRecord.setTag("process", processName);
      agentActivityMBean = new AgentActivityMBean(registry, recordName);
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
    if (agentActivityMBean != null)
      agentActivityMBean.shutdown();
  }

}
