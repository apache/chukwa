package org.apache.hadoop.chukwa.util;

import java.sql.ResultSet;
import java.util.Calendar;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.chukwa.util.DatabaseWriter;
import org.apache.hadoop.chukwa.database.DatabaseConfig;
public class WatchDog {
    private static Log log = LogFactory.getLog(WatchDog.class);
    private String cluster=null;
    
    public WatchDog(String cluster){
        this.cluster=cluster;
    }
    
                            
    public void run(){
       long updates = 0;
       boolean error = false;
       try{
           // SQL query to monitor database
           DatabaseConfig dbc = new DatabaseConfig();
           log.info("cluster:"+cluster);
           DatabaseWriter db = new DatabaseWriter(cluster);
           Calendar c = Calendar.getInstance();
           long now = c.getTimeInMillis();
           String[] tableName = dbc.findTableName("system_metrics", now, now);
           String query = "select unix_TIMESTAMP(now()) - unix_timestamp(max(timestamp)) as delay from "+tableName[0]+" ;";
           ResultSet rs = db.query(query);
           while(rs.next()) {
               long delay = rs.getLong(1);
               if(delay>600) {
                   log.error("Chukwa: "+cluster+": No new data for the past 30 minutes for system metrics");                   
                   error=true;
               }
           }
           query = "select count(*) as UpdatesPerHr from "+tableName[0]+" where Timestamp > date_sub(now(), interval 60 minute) ;";
           rs = db.query(query);
           while(rs.next()) {
               updates = rs.getLong(1);
               if(updates==0) {
                   log.error("Chukwa: "+cluster+": No system metrics data received for the past 60 minutes");                   
                   error=true;
               }
           }
           String[] hodTableNames = dbc.findTableName("HodJob", now, now);
           query = "select count(*) as UpdatesPerHr from "+hodTableNames[0]+" where StartTime > date_sub(now(), interval 60 minute) ;";
           rs = db.query(query);           
           while(rs.next()) {
               long updatesHod = rs.getLong(1);
               if(updatesHod==0) {
                   log.error("Chukwa: "+cluster+": No hod job data received for the past 60 minutes");
               }
           }
           String[] mrTableNames = dbc.findTableName("mr_job", now, now);
           query = "select count(*) as UpdatesPerHr from "+mrTableNames+" where FINISH_TIME > date_sub(now(), interval 1440 minute) ;";
           rs = db.query(query);                      
           while(rs.next()) {
               long updatesMR = rs.getLong(1);
               if(updatesMR==0) {
                   log.error("MDL: no map reduce job data received for the past day.");
                   error=true;
               }
           }
           db.close();
       }catch (Exception ex){
           log.error("Unexpected error:"+ex.getStackTrace().toString());
           System.exit(1);
       }
       if(!error) {
           log.info("MDL: Status OK");
       }
           
    }
    
    
     public static void main(String[] args) {
         String cluster = System.getProperty("CLUSTER");
         if(cluster!=null) {
             WatchDog wd = new WatchDog(cluster);
             wd.run();
         } else {
             log.error("Chukwa: jdbc.conf is not configured");
         }
     }        
}
