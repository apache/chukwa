package org.apache.hadoop.chukwa.hicc.rest;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.hadoop.chukwa.datacollection.adaptor.sigar.SigarRunner;
import org.apache.hadoop.chukwa.datastore.ChukwaHBaseStore;
import org.apache.hadoop.chukwa.hicc.TimeHandler;
import org.apache.hadoop.chukwa.hicc.bean.Series;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;

@Path("/metrics")
public class MetricsController {
  private static Logger log = Logger.getLogger(MetricsController.class);

  @GET
  @Path("series/{table}/{column}/rowkey/{rkey}")
  @Produces("application/json")
  public String getSeries(@Context HttpServletRequest request, @PathParam("table") String table, @PathParam("column") String column, @PathParam("rkey") String rkey, @QueryParam("start") String start, @QueryParam("end") String end) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    String buffer = "";
    Series series;
    long startTime = 0;
    long endTime = 0;
    TimeHandler time = new TimeHandler(request);
    try {
      if(start!=null) {
        startTime = sdf.parse(start).getTime();
      } else {
        startTime = time.getStartTime();
      }
      if(end!=null) {
        endTime = sdf.parse(end).getTime();
      } else {
        endTime = time.getEndTime();
      }
      if(rkey!=null) {
        series = ChukwaHBaseStore.getSeries(table, rkey, column, startTime, endTime, true);
        buffer = series.toString();
      } else {
        throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
            .entity("No row key defined.").build());
      }
    } catch (ParseException e) {
      throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
          .entity("Start/End date parse error.  Format: yyyyMMddHHmmss.").build());
    }
    return buffer;
  }

  @GET
  @Path("series/{table}/{column}/session/{sessionKey}")
  @Produces("application/json")
  public String getSeriesBySessionAttribute(@Context HttpServletRequest request, @PathParam("table") String table, @PathParam("column") String column, @PathParam("sessionKey") String skey, @QueryParam("start") String start, @QueryParam("end") String end) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    String buffer = "";
    Series series;
    long startTime = 0;
    long endTime = 0;
    TimeHandler time = new TimeHandler(request);
    try {
      if(start!=null) {
        startTime = sdf.parse(start).getTime();
      } else {
        startTime = time.getStartTime();
      }
      if(end!=null) {
        endTime = sdf.parse(end).getTime();
      } else {
        endTime = time.getEndTime();
      }
      if(skey!=null) {
          HttpSession session = request.getSession();
          String[] rkeys = (session.getAttribute(skey).toString()).split(",");
          JSONArray seriesList = new JSONArray();
          for(String rowKey : rkeys) {
            Series output = ChukwaHBaseStore.getSeries(table, rowKey, column, startTime, endTime, true);
            seriesList.add(output.toJSONObject());
          }
          buffer = seriesList.toString();
      } else {
        throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
            .entity("No session attribute key defined.").build());
      }
    } catch (ParseException e) {
      throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
          .entity("Start/End date parse error.  Format: yyyyMMddHHmmss.").build());
    }
    return buffer;
  }

  @GET
  @Path("schema")
  @Produces("application/json")
  public String getTables() {
    Set<String> tableNames = ChukwaHBaseStore.getTableNames();
    JSONArray tables = new JSONArray();
    for(String table : tableNames) {
      tables.add(table);
    }
    return tables.toString();
  }
  
  @GET
  @Path("schema/{table}")
  @Produces("application/json")
  public String getFamilies(@PathParam("table") String tableName) {
    Set<String> familyNames = ChukwaHBaseStore.getFamilyNames(tableName);
    JSONArray families = new JSONArray();
    for(String family : familyNames) {
      families.add(family);
    }
    return families.toString();
  }
  
  @GET
  @Path("schema/{table}/{family}")
  @Produces("application/json")
  public String getColumnNames(@Context HttpServletRequest request, @PathParam("table") String tableName, @PathParam("family") String family, @QueryParam("start") String start, @QueryParam("end") String end, @DefaultValue("false") @QueryParam("fullScan") boolean fullScan) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    String buffer = "";
    Series series;
    long startTime = 0;
    long endTime = 0;
    TimeHandler time = new TimeHandler(request);
    try {
      if(start!=null) {
        startTime = sdf.parse(start).getTime();
      } else {
        startTime = time.getStartTime();
      }
      if(end!=null) {
        endTime = sdf.parse(end).getTime();
      } else {
        endTime = time.getEndTime();
      }
    } catch(ParseException e) {
      throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
          .entity("Start/End date parse error.  Format: yyyyMMddHHmmss.").build());      
    }
    Set<String> columnNames = ChukwaHBaseStore.getColumnNames(tableName, family, startTime, endTime, fullScan);
    JSONArray columns = new JSONArray();
    for(String column : columnNames) {
      columns.add(column);
    }
    return columns.toString();
  }
  
  @GET
  @Path("rowkey/{table}/{column}")
  @Produces("application/json")
  public String getRowNames(@Context HttpServletRequest request, @PathParam("table") String tableName, @PathParam("family") String family, @PathParam("column") String column, @QueryParam("start") String start, @QueryParam("end") String end, @QueryParam("fullScan") @DefaultValue("false") boolean fullScan) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    String buffer = "";
    Series series;
    long startTime = 0;
    long endTime = 0;
    TimeHandler time = new TimeHandler(request);
    try {
      if(start!=null) {
        startTime = sdf.parse(start).getTime();
      } else {
        startTime = time.getStartTime();
      }
      if(end!=null) {
        endTime = sdf.parse(end).getTime();
      } else {
        endTime = time.getEndTime();
      }
    } catch(ParseException e) {
      throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
          .entity("Start/End date parse error.  Format: yyyyMMddHHmmss.").build());      
    }
    Set<String> columnNames = ChukwaHBaseStore.getRowNames(tableName, column, startTime, endTime, fullScan);
    JSONArray rows = new JSONArray();
    for(String row : columnNames) {
      rows.add(row);
    }
    return rows.toString();
  }
}
