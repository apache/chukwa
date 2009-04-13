package org.apache.hadoop.chukwa.database;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.util.DatabaseWriter;

public class Macro {
    private static Log log = LogFactory.getLog(Macro.class);
    private boolean forCharting = false;
    private long current = 0;
    private long start = 0;
    private long end = 0;
    private static DatabaseConfig dbc = new DatabaseConfig();
    private static DatabaseWriter db = null;
    private String query = null;
    private HttpServletRequest request = null;

    public Macro(long timestamp, String query) {
        this.current = timestamp;
        this.start = timestamp;
        this.end = timestamp;
        this.query = query;
    }

    public Macro(long startTime, long endTime, String query) {
        this.current = endTime;
        this.start = startTime;
        this.end = endTime;
        forCharting = true;	
        this.query = query;
    }
    
    public Macro(long startTime, long endTime, String query, HttpServletRequest request) {
        this.request = request;
        this.current = endTime;
        this.start = startTime;
        this.end = endTime;
        forCharting = true; 
        this.query = query;        
    }
    public HashMap<String,String> findMacros(String query) throws SQLException {
        boolean add=false;
        HashMap<String,String> macroList = new HashMap<String,String>();
        String macro="";
        for(int i=0;i<query.length();i++) {
            if(query.charAt(i)==']') {
                add=false;
                if(!macroList.containsKey(macro)) {
                    String subString = computeMacro(macro);
                    macroList.put(macro,subString);	    			
                }
                macro="";
            }
            if(add) {
                macro=macro+query.charAt(i);
            }
            if(query.charAt(i)=='[') {
                add=true;
            }
        }
        return macroList;
    }

    public String computeMacro(String macro) throws SQLException {
        Pattern p = Pattern.compile("past_(.*)_minutes");
        Matcher matcher = p.matcher(macro);
        if(macro.indexOf("avg(")==0 || macro.indexOf("group_avg(")==0 || macro.indexOf("sum(")==0) {
            String meta="";
            String[] table = null;
            if(forCharting) {
                table = dbc.findTableNameForCharts(macro.substring(macro.indexOf("(")+1,macro.indexOf(")")), start, end);
            } else {
                table = dbc.findTableName(macro.substring(macro.indexOf("(")+1,macro.indexOf(")")), start, end);
            }
            try {
                String cluster = System.getProperty("CLUSTER");
                if(cluster==null) {
                    cluster="unknown";
                }
                if(db==null) {
                    db = new DatabaseWriter(cluster);
                }
                DatabaseMetaData dbMetaData = db.getConnection().getMetaData();
                ResultSet rs = dbMetaData.getColumns ( null,null,table[0], null);
                boolean first=true;
                while(rs.next()) {
                    if(!first) {
                        meta = meta+",";
                    }
                    String name = rs.getString(4);
                    int type = rs.getInt(5);
                    if(type==java.sql.Types.VARCHAR) {
                        if(macro.indexOf("group_avg(")<0) {
                            meta=meta+"count("+name+") as "+name;
                        } else {
                            meta=meta+name;
                        }
                        first=false;
                    } else if(type==java.sql.Types.DOUBLE ||
                            type==java.sql.Types.FLOAT ||
                            type==java.sql.Types.INTEGER) {
                        if(macro.indexOf("sum(")==0) {
                            meta=meta+"sum("+name+")";	            			
                        } else {
                            meta=meta+"avg("+name+")";
                        }
                        first=false;
                    } else if(type==java.sql.Types.TIMESTAMP) {
                        // Skip the column
                    } else {
                        if(macro.indexOf("sum(")==0) {
                            meta=meta+"SUM("+name+")";
                        } else {
                            meta=meta+"AVG("+name+")";	            			
                        }
                        first=false;
                    }
                }
                db.close();
                if(first) {
                    throw new SQLException("Table is undefined.");
                }
            } catch(SQLException ex) {
                throw new SQLException("Table does not exist:"+ table[0]);
            }
            return meta;
        } else if(macro.indexOf("now")==0) {
            SimpleDateFormat sdf = new SimpleDateFormat();
            return DatabaseWriter.formatTimeStamp(current);
        } else if(macro.intern()=="start".intern()) {
            return DatabaseWriter.formatTimeStamp(start);
        } else if(macro.intern()=="end".intern()) {
            return DatabaseWriter.formatTimeStamp(end);
        } else if(matcher.find()) {
            int period = Integer.parseInt(matcher.group(1));
            long timestamp = current - (current % (period*60*1000L)) - (period*60*1000L);
            return DatabaseWriter.formatTimeStamp(timestamp);
        } else if(macro.indexOf("past_hour")==0) {
            return DatabaseWriter.formatTimeStamp(current-3600*1000L);
        } else if(macro.endsWith("_week")) {
            long partition = current / DatabaseConfig.WEEK;
            if(partition<=0) {
                partition=1;
            }
            String[] buffers = macro.split("_");
            StringBuffer tableName = new StringBuffer();
            for(int i=0;i<buffers.length-1;i++) {
                tableName.append(buffers[i]);
                tableName.append("_");
            }
            tableName.append(partition);
            tableName.append("_week");
            return tableName.toString();
        } else if(macro.endsWith("_month")) {
            long partition = current / DatabaseConfig.MONTH;
            if(partition<=0) {
                partition=1;
            }
            String[] buffers = macro.split("_");
            StringBuffer tableName = new StringBuffer();
            for(int i=0;i<buffers.length-1;i++) {
                tableName.append(buffers[i]);
                tableName.append("_");
            }
            tableName.append(partition);
            tableName.append("_month");
            return tableName.toString();
        } else if(macro.endsWith("_quarter")) {
            long partition = current / DatabaseConfig.QUARTER;
            if(partition<=0) {
                partition=1;
            }
            String[] buffers = macro.split("_");
            StringBuffer tableName = new StringBuffer();
            for(int i=0;i<buffers.length-1;i++) {
                tableName.append(buffers[i]);
                tableName.append("_");
            }
            tableName.append(partition);
            tableName.append("_quarter");
            return tableName.toString();
        } else if(macro.endsWith("_year")) {
            long partition = current / DatabaseConfig.YEAR;
            if(partition<=0) {
                partition=1;
            }
            String[] buffers = macro.split("_");
            StringBuffer tableName = new StringBuffer();
            for(int i=0;i<buffers.length-1;i++) {
                tableName.append(buffers[i]);
                tableName.append("_");
            }
            tableName.append(partition);
            tableName.append("_year");
            return tableName.toString();
        } else if(macro.endsWith("_decade")) {
            long partition = current / DatabaseConfig.DECADE;
            if(partition<=0) {
                partition=1;
            }
            String[] buffers = macro.split("_");
            StringBuffer tableName = new StringBuffer();
            for(int i=0;i<buffers.length-1;i++) {
                tableName.append(buffers[i]);
                tableName.append("_");
            }
            tableName.append(partition);
            tableName.append("_decade");
            return tableName.toString();
        }
        if(forCharting) {
            if(macro.startsWith("session(") && request!=null){
                String keyword = macro.substring(macro.indexOf("(")+1,macro.indexOf(")"));
                String[] objects = null;
                if(request.getSession().getAttribute(keyword)!=null) {
                    objects = ((String)request.getSession().getAttribute(keyword)).split(",");
                }
                StringBuffer buf = new StringBuffer();
                boolean first = true;
                if(objects!=null) {
                    for(String object : objects) {
                        if(!first) {
                            buf.append(" or ");
                        }
                        first = false;
                        buf.append(macro.substring(macro.indexOf("(")+1,macro.indexOf(")"))+"='"+object+"'");
                    }
                    return buf.toString();
                }
                return "";
            } else {
                String[] tableList = dbc.findTableNameForCharts(macro, start, end);
                StringBuffer buf = new StringBuffer();
                boolean first = true;
                for(String table : tableList) {
                    if(!first) {
                        buf.append("|");
                    }
                    first = false;
                    buf.append(table);
                }
                return buf.toString();
            }
        }
        String[] tableList = dbc.findTableName(macro,current,current);
        return tableList[0];
    }
    public String toString() {
        try {
        HashMap<String, String> macroList = findMacros(query);
        Iterator<String> macroKeys = macroList.keySet().iterator();
        while(macroKeys.hasNext()) {
            String mkey = macroKeys.next();
            if(macroList.get(mkey).contains("|")) {
                StringBuffer buf = new StringBuffer();
                String[] tableList = macroList.get(mkey).split("\\|");
                boolean first = true;
                for(String table : tableList) {
                    String newQuery = query.replace("["+mkey+"]", table);
                    if(!first) {
                        buf.append(" union ");
                    }
                    buf.append("(");
                    buf.append(newQuery);
                    buf.append(")");
                    first = false;
                }
                query = buf.toString();
            } else {
                log.debug("replacing:"+mkey+" with "+macroList.get(mkey));
                query = query.replace("["+mkey+"]", macroList.get(mkey));
            }
        }
        } catch(SQLException ex) {
            log.error(query);
            log.error(ex.getMessage());
        }
        return query;
    }

}
