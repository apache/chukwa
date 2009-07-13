/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.hadoop.chukwa.analysis.salsa.visualization;

import prefuse.data.io.sql.*;
import prefuse.data.Table;
import prefuse.data.expression.parser.*;
import prefuse.data.expression.*;
import prefuse.data.column.*;
import prefuse.data.query.*;
import prefuse.data.*;
import prefuse.action.*;
import prefuse.action.layout.*;
import prefuse.action.assignment.*;
import prefuse.visual.expression.*;
import prefuse.visual.*;
import prefuse.render.*;
import prefuse.util.*;
import prefuse.*;

import org.apache.hadoop.chukwa.hicc.OfflineTimeHandler;
import org.apache.hadoop.chukwa.hicc.TimeHandler;
import org.apache.hadoop.chukwa.util.DatabaseWriter;
import org.apache.hadoop.chukwa.database.Macro;
import org.apache.hadoop.chukwa.util.XssFilter;

import javax.servlet.http.*;
import javax.swing.BorderFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.*;
import java.text.NumberFormat;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import java.awt.Font;
import java.awt.geom.Rectangle2D;
import java.awt.Color;

/**
 * Static image rendering for heatmap visualization of spatial HDFS 
 * activity patterns for scalable rendering on front-end (web-browser)
 * Handles database data retrieval, transforming data to form for 
 * visualization elements, and initializing and calling visualization
 * elements 
 */
public class Heatmap {

  /**
   * Internal representation of all data needed to render heatmap;
   * data-handling code populates this data structure
   */
  protected static class HeatmapData {
    public Table agg_tab;
    public long [][] stats;
    public long min;
    public long max;
    public int num_hosts;
    public String [] hostnames;
    public HeatmapData() {
    }
  }

  private static Log log = LogFactory.getLog(Heatmap.class);

  static final String START_FIELD_NAME = "start_time_num";
  static final String END_FIELD_NAME = "finish_time_num";

  int BOXWIDTH = 250;
  int SIZE_X = 1600, SIZE_Y=1600;
  final int [] BORDER = {200,150,150,150};
  final int LEGEND_X_OFFSET = 10;
  final int LEGEND_Y_OFFSET = 0;
  final int LEGEND_TEXT_OFFSET = 10;
  final int LEGEND_FONT_SIZE = 24;
  final int AXIS_NAME_FONT_SIZE = 24;

  protected boolean offline_use = true;
  protected HttpServletRequest request;

  // for offline use only
  // keys that need to be filled:
  // period (last1/2/3/6/12/24hr,last7d,last30d), time_type (range/last), start, end
  protected HashMap<String, String> param_map;
  
  protected String cluster;
  protected String timezone;
  protected String query_state;
  protected String query_stat_type;
  protected final String table = new String("filesystem_fsm");
  protected boolean plot_legend = false; // controls whether to plot hostnames
  protected boolean sort_nodes = true;
  protected boolean plot_additional_info = true;
  protected String add_info_extra = null;
  
  protected Display dis;
  protected Visualization viz;
  
  protected Rectangle2D dataBound = new Rectangle2D.Double();
  protected Rectangle2D xlabBound = new Rectangle2D.Double();
  protected Rectangle2D ylabBound = new Rectangle2D.Double();
  protected Rectangle2D labelBottomBound = new Rectangle2D.Double();
  
  protected HashMap<String, String> prettyStateNames;
  
  /* Different group names allow control of what Renderers to use */
  final String maingroup = "Data";
  final String othergroup = "Misc";
  final String labelgroup = "Label";
  final String legendgroup = "Legend";
  final String legendshapegroup = "LegendShape";
  final String addinfogroup = "AddInfo";
  final String addinfoshapegroup = "AddInfoShape";
  
  public Heatmap() {
    this.cluster = new String("");
    this.timezone = new String("");
    this.query_state = new String("");
    this.query_stat_type = new String("");
    param_map = new HashMap<String, String>();    
  }
  
  /**
   * @brief Constructor for Swimlanes visualization object
   * @param timezone Timezone string from environment
   * @param cluster Cluster name from environment
   * @param event_type Whether to display shuffles or not
   * @param valmap HashMap of key/value pairs simulating parameters from a HttpRequest
   */
  public Heatmap
    (String timezone, String cluster, String event_type, 
     String query_stat_type,
     HashMap<String, String> valmap) 
  {
    this.cluster = new String(cluster);
    if (timezone != null) {
      this.timezone = new String(timezone);
    } else {
      this.timezone = null;
    }    
    this.query_state = new String(event_type);
    this.query_stat_type = new String(query_stat_type);

    /* This should "simulate" an HttpServletRequest
     * Need to have "start" and "end" in seconds since Epoch
     */
    this.param_map = valmap; 
  }
  
  public Heatmap
    (String timezone, String cluster, String query_state, 
     String query_stat_type,
     HashMap<String, String> valmap, String shuffles) 
  {
    
    this.cluster = new String(cluster);
    if (timezone != null) {
      this.timezone = new String(timezone);
    } else {
      this.timezone = null;
    }
    this.query_state = new String(query_state);
    this.query_stat_type = new String(query_stat_type);

    /* This should "simulate" an HttpServletRequest
     * Need to have "start" and "end" in seconds since Epoch
     */
    this.param_map = valmap; 
    
  }
  
  public Heatmap
    (String timezone, String cluster, String query_state, 
     String query_stat_type,
     HashMap<String, String> valmap, 
     int w, int h) 
  {
    
    this.cluster = new String(cluster);
    if (timezone != null) {
      this.timezone = new String(timezone);
    } else {
      this.timezone = null;
    }
    this.query_state = new String(query_state);
    this.query_stat_type = new String(query_stat_type);

    /* This should "simulate" an HttpServletRequest
     * Need to have "start" and "end" in seconds since Epoch
     */
    this.param_map = valmap; 
        
    this.SIZE_X = w;
    this.SIZE_Y = h;
  }
  
  public Heatmap(HttpServletRequest request) {
    XssFilter xf = new XssFilter(request);
    this.offline_use = false;
    this.request = request;
    HttpSession session = request.getSession();
    this.cluster = session.getAttribute("cluster").toString();
    String query_state = xf.getParameter("query_state");
    if (query_state != null) {
      this.query_state = new String(query_state);
    } else {
      this.query_state = new String("read");
    }
    String query_stat_type = xf.getParameter("query_stat_type");
    if (query_stat_type != null) {
      this.query_stat_type = new String(query_stat_type);
    } else {
      this.query_stat_type = new String("transaction_count");
    }
    this.timezone = session.getAttribute("time_zone").toString();
  }

  /**
   * Set dimensions of image to be generated
   * Call before calling @see #run
   */
  public void setDimensions(int width, int height) {
    this.SIZE_X=width;
    this.SIZE_Y=height;
  }
  
  /**
   * Specify whether to print labels of hosts along axes
   * Call before calling @see #run
   */
  public void setLegend(boolean legendopt) {
    if (legendopt) {
      this.plot_legend = true;
    } else {
      this.plot_legend = false;
    }
  }
  
  
  /**
   * Generates image in specified format, and writes image as binary
   * output to supplied output stream 
   */
  public boolean getImage(java.io.OutputStream output, String img_fmt, double scale) {
    dis = new Display(this.viz);
    dis.setSize(SIZE_X,SIZE_Y);
    dis.setHighQuality(true);
    dis.setFont(new Font(Font.SANS_SERIF,Font.PLAIN,24));
    return dis.saveImage(output, img_fmt, scale);
  } 
  
  protected void setupRenderer() {
    this.viz.setRendererFactory(new RendererFactory(){
      AbstractShapeRenderer sr = new ShapeRenderer();
      ShapeRenderer sr_big = new ShapeRenderer(BOXWIDTH);
      Renderer arY = new AxisRenderer(Constants.LEFT, Constants.TOP);
      Renderer arX = new AxisRenderer(Constants.CENTER, Constants.BOTTOM);
      PolygonRenderer pr = new PolygonRenderer(Constants.POLY_TYPE_LINE);
      LabelRenderer lr = new LabelRenderer("label");
      LabelRenderer lr_legend = new LabelRenderer("label");
            
      public Renderer getRenderer(VisualItem item) {
        lr_legend.setHorizontalAlignment(Constants.LEFT);
        lr_legend.setVerticalAlignment(Constants.CENTER);
        lr.setHorizontalAlignment(Constants.CENTER);
        lr.setVerticalAlignment(Constants.CENTER);
        if (item.isInGroup(maingroup)) {
          return sr_big;
        } else if (item.isInGroup(legendgroup)) {
          return lr_legend;
        } else if (item.isInGroup(addinfogroup)) {
          return lr;
        }
        return sr;
      }
    });
  }
  
  // setup columns: add additional time fields
  protected HeatmapData setupDataTable() {
    HeatmapData hd = this.getData();    
    return hd;
  }
  
  protected void setupHeatmap(VisualTable vtab, HeatmapData hd) 
  {
    long [][] stats = hd.stats;
    int i, j, curr_idx;
    long curr_val;
    int num_hosts = hd.num_hosts;
    ColorMap cm = new ColorMap(
      ColorLib.getInterpolatedPalette(
        ColorLib.color(ColorLib.getColor(32,0,0)),
        ColorLib.color(Color.WHITE)
      ),
      (double)hd.min,(double)hd.max
    );
    
    for (i = 0; i < num_hosts; i++) {
      for (j = 0; j < num_hosts; j++) {
        curr_idx = j+(i*num_hosts);
        curr_val = stats[i][j]; 
        if (curr_val >= hd.min) {
          vtab.setFillColor(curr_idx, cm.getColor((double)curr_val));
        } else if (curr_val == 0) {
          vtab.setFillColor(curr_idx, ColorLib.color(Color.BLACK));
        }
      }
    }
    
    // gridlayout puts tiles on row-wise (row1, followed by row2, etc.)
    GridLayout gl = new GridLayout(maingroup, num_hosts, num_hosts);
    gl.setLayoutBounds(this.dataBound);
    ActionList gl_list = new ActionList();
    gl_list.add(gl);
    this.viz.putAction("gridlayout",gl_list);
    this.viz.run("gridlayout");
  }
  
  protected void addHostLabels(HeatmapData hd) {
    Table legend_labels_table = new Table();
    legend_labels_table.addColumn("label",String.class);
    legend_labels_table.addRows(hd.hostnames.length);
    for (int i = 0; i < hd.hostnames.length; i++) {
      legend_labels_table.setString(i,"label",hd.hostnames[i]);
    }
    float start_x = LEGEND_X_OFFSET;
    float start_y = LEGEND_Y_OFFSET + BORDER[1] + (BOXWIDTH/2);    
    float incr = this.BOXWIDTH;
    VisualTable legend_labels_table_viz = this.viz.addTable(legendgroup, legend_labels_table);
    for (int i = 0; i < hd.hostnames.length; i++) {
      legend_labels_table_viz.setFloat(i, VisualItem.X, start_x + LEGEND_TEXT_OFFSET);
      legend_labels_table_viz.setFloat(i, VisualItem.Y, start_y + (i * incr));
      legend_labels_table_viz.setTextColor(i,ColorLib.color(java.awt.Color.BLACK));
      legend_labels_table_viz.setFont(i,new Font(Font.SANS_SERIF,Font.PLAIN,LEGEND_FONT_SIZE));
    } 
  }
  
  protected void addAddlInfo(HeatmapData hd) {
    Table legend_labels_table = new Table();
    legend_labels_table.addColumn("label",String.class);
    legend_labels_table.addRows(3);
    
    String hostnumstring = "Number of hosts: " + hd.num_hosts;
    if (sort_nodes) {
      hostnumstring += " (nodes sorted)";
    } else {
      hostnumstring += " (nodes not sorted)";
    }
    if (add_info_extra != null) hostnumstring += add_info_extra;
    legend_labels_table.setString(0,"label",hostnumstring);
    legend_labels_table.setString(1,"label","Src. Hosts");
    legend_labels_table.setString(2,"label","Dest. Hosts");
    
    float start_x = LEGEND_X_OFFSET;
    float start_y = LEGEND_Y_OFFSET + BORDER[1] + (BOXWIDTH/2);    
    float incr = this.BOXWIDTH;
    VisualTable legend_labels_table_viz = this.viz.addTable(addinfogroup, legend_labels_table);

    legend_labels_table_viz.setFloat(0, VisualItem.X, this.SIZE_X/2);
    legend_labels_table_viz.setFloat(0, VisualItem.Y, BORDER[1]/2);
    legend_labels_table_viz.setTextColor(0,ColorLib.color(java.awt.Color.BLACK));
    legend_labels_table_viz.setFont(0,new Font(Font.SANS_SERIF,Font.PLAIN,LEGEND_FONT_SIZE));

    legend_labels_table_viz.setFloat(1, VisualItem.X, this.SIZE_X/2);
    legend_labels_table_viz.setFloat(1, VisualItem.Y, BORDER[1] + (BOXWIDTH*hd.num_hosts) + BORDER[3]/2);
    legend_labels_table_viz.setTextColor(1,ColorLib.color(java.awt.Color.BLACK));
    legend_labels_table_viz.setFont(1,new Font(Font.SANS_SERIF,Font.PLAIN,LEGEND_FONT_SIZE));

    legend_labels_table_viz.setFloat(2, VisualItem.X, BORDER[0] + (BOXWIDTH*hd.num_hosts) + BORDER[2]/2);
    legend_labels_table_viz.setFloat(2, VisualItem.Y, this.SIZE_Y/2);
    legend_labels_table_viz.setTextColor(2,ColorLib.color(java.awt.Color.BLACK));
    legend_labels_table_viz.setFont(2,new Font(Font.SANS_SERIF,Font.PLAIN,LEGEND_FONT_SIZE));

  }
  
  protected void initPrettyNames() {
    this.prettyStateNames = new HashMap<String, String>();

    prettyStateNames.put("read","Block Reads");
    prettyStateNames.put("write","Block Writes");
    prettyStateNames.put("read_local", "Local Block Reads");
    prettyStateNames.put("write_local", "Local Block Writes");
    prettyStateNames.put("read_remote", "Remote Block Reads");
    prettyStateNames.put("write_remote", "Remote Block Writes");
    prettyStateNames.put("write_replicated", "Replicated Block Writes");    
  }
  
  /**
   * Actual code that calls data, generates heatmap, and saves it
   */
  public void run() {
    initPrettyNames();
    
    // setup visualization
    this.viz = new Visualization();
    
    // add table to visualization
    HeatmapData hd = this.setupDataTable();

    // setup bounds
    int width, realwidth;
    if (SIZE_X-BORDER[0]-BORDER[2] < SIZE_Y-BORDER[1]-BORDER[3]) {
      BOXWIDTH = (SIZE_X-BORDER[0]-BORDER[2]) / hd.num_hosts;
    } else {
      BOXWIDTH = (SIZE_Y-BORDER[1]-BORDER[3]) / hd.num_hosts;
    }
    width = hd.num_hosts * BOXWIDTH;
    this.dataBound.setRect(
      BORDER[0]+BOXWIDTH/2,
      BORDER[1]+BOXWIDTH/2,
      width-BOXWIDTH,width-BOXWIDTH
    );
    this.SIZE_X = BORDER[0] + BORDER[2] + (hd.num_hosts * BOXWIDTH);
    this.SIZE_Y = BORDER[1] + BORDER[3] + (hd.num_hosts * BOXWIDTH);
    
    log.debug("width total: " + width + " width per state: " + BOXWIDTH + " xstart: " 
      + (BORDER[0]+BOXWIDTH/2) 
      + " ystart: " + (BORDER[1]+BOXWIDTH/2) + " (num hosts: "+hd.num_hosts+")");
    log.debug("X size: " + this.SIZE_X + " Y size: " + this.SIZE_Y);
    
    this.setupRenderer();
    VisualTable data_tab_viz = viz.addTable(maingroup, hd.agg_tab);
    setupHeatmap(data_tab_viz, hd);
    
    ShapeAction legend_sa1 = null, legend_sa2 = null;
    SpecifiedLayout legendlabels_sl1 = null, legendlabels_sl2 = null;
    
    if (plot_legend) {
      addHostLabels(hd);
      legend_sa1 = new ShapeAction(legendshapegroup);
      legendlabels_sl1 = new SpecifiedLayout(legendgroup, VisualItem.X, VisualItem.Y);
      ActionList legenddraw = new ActionList();
      legenddraw.add(legend_sa1);
      this.viz.putAction(legendshapegroup, legenddraw);
      ActionList legendlabelsdraw = new ActionList();
      legendlabelsdraw.add(legendlabels_sl1);
      this.viz.putAction(legendgroup,legendlabelsdraw);
    }

    if (plot_additional_info) {
      addAddlInfo(hd);
      legend_sa2 = new ShapeAction(addinfoshapegroup);
      legendlabels_sl2 = new SpecifiedLayout(addinfogroup, VisualItem.X, VisualItem.Y);    
      ActionList legenddraw = new ActionList();
      legenddraw.add(legend_sa2);
      this.viz.putAction(addinfoshapegroup, legenddraw);
      ActionList legendlabelsdraw = new ActionList();
      legendlabelsdraw.add(legendlabels_sl2);
      this.viz.putAction(addinfogroup,legendlabelsdraw);
    }

  }
  
  protected boolean checkDone(int [] clustId) {
    for (int i = 1; i < clustId.length; i++) {
      if (clustId[i] != clustId[0]) return false;
    }
    return true;
  }
  
  /**
   * Sort data for better visualization of patterns
   */
  protected int [] hClust (long [][] stat) 
  {
    int statlen = stat.length;
    long [] rowSums = new long[statlen];
    int [] permute = new int[statlen];
    int i,j,k;

    // initialize permutation
    for (i = 0; i < statlen; i++) {
      permute[i] = i;
    }
    
    for (i = 0; i < statlen; i++) {
      rowSums[i] = 0;
      for (j = 0; j < statlen; j++) {
        rowSums[i] += stat[i][j];
      }
    }
    
    // insertion sort
    for (i = 0; i < statlen-1; i++) {
      long val = rowSums[i];
      int thispos = permute[i];
      j = i-1;
      while (j >= 0 && rowSums[j] > val) {
        rowSums[j+1] = rowSums[j];
        permute[j+1] = permute[j];
        j--;
      }
      rowSums[j+1] = val; 
      permute[j+1] = thispos;
    }
    
    return permute;
      
  }
  
  /**
   * Reorder rows (and columns) according to a given ordering
   * Maintains same ordering along rows and columns
   */
  protected long [][] doPermute (long [][] stat, int [] permute) {
    int statlen = stat.length;
    int i, j, curr_pos;
    long [][] stat2 = new long[statlen][statlen];
    
    assert(stat.length == permute.length);
    
    for (i = 0; i < statlen; i++) {
      curr_pos = permute[i];
      for (j = 0; j < statlen; j++) {
        stat2[i][j] = stat[curr_pos][permute[j]];
      }
    }
    
    return stat2;
  }
  
  /**
   * Interfaces with database to get data and 
   * populate data structures for rendering
   */
  public HeatmapData getData() {
    // preliminary setup
    OfflineTimeHandler time_offline;
    TimeHandler time_online;
    long start, end, min, max;
    
    if (offline_use) {
      time_offline = new OfflineTimeHandler(param_map, this.timezone);
      start = time_offline.getStartTime();
      end = time_offline.getEndTime();
    } else {
      time_online = new TimeHandler(this.request, this.timezone);
      start = time_online.getStartTime();
      end = time_online.getEndTime();
    }
    
    DatabaseWriter dbw = new DatabaseWriter(this.cluster);
    
    // setup query
    String query;
    if (this.query_state != null && this.query_state.equals("read")) {
      query = "select block_id,start_time,finish_time,start_time_millis,finish_time_millis,status,state_name,hostname,other_host,bytes from ["+table+"] where finish_time between '[start]' and '[end]' and (state_name like 'read_local' or state_name like 'read_remote')";
    } else if (this.query_state != null && this.query_state.equals("write")) {
      query = "select block_id,start_time,finish_time,start_time_millis,finish_time_millis,status,state_name,hostname,other_host,bytes from ["+table+"] where finish_time between '[start]' and '[end]' and (state_name like 'write_local' or state_name like 'write_remote' or state_name like 'write_replicated')";
    } else {
      query = "select block_id,start_time,finish_time,start_time_millis,finish_time_millis,status,state_name,hostname,other_host,bytes from ["+table+"] where finish_time between '[start]' and '[end]' and state_name like '" + query_state + "'";
    } 
    Macro mp = new Macro(start,end,query);
    query = mp.toString() + " order by start_time";
    
    ArrayList<HashMap<String, Object>> events = new ArrayList<HashMap<String, Object>>();

    ResultSet rs = null;
    
    log.debug("Query: " + query);
    // run query, extract results
    try {
      rs = dbw.query(query);
      ResultSetMetaData rmeta = rs.getMetaData();
      int col = rmeta.getColumnCount();
      while (rs.next()) {
        HashMap<String, Object> event = new HashMap<String, Object>();
        long event_time=0;
        for(int i=1;i<=col;i++) {
          if(rmeta.getColumnType(i)==java.sql.Types.TIMESTAMP) {
            event.put(rmeta.getColumnName(i),rs.getTimestamp(i).getTime());
          } else {
            event.put(rmeta.getColumnName(i),rs.getString(i));
          }
        }
        events.add(event);
      }
    } catch (SQLException ex) {
      // handle any errors
      log.error("SQLException: " + ex.getMessage());
      log.error("SQLState: " + ex.getSQLState());
      log.error("VendorError: " + ex.getErrorCode());
    } finally {
      dbw.close();
    }    
    SimpleDateFormat format = new SimpleDateFormat("MMM dd yyyy HH:mm:ss");

    log.info(events.size() + " results returned.");

    HashSet<String> host_set = new HashSet<String>();
    HashMap<String, Integer> host_indices = new HashMap<String, Integer>();
    HashMap<Integer, String> host_rev_indices = new HashMap<Integer, String>();

    // collect hosts, name unique hosts
    for(int i = 0; i < events.size(); i++) {
      HashMap<String, Object> event = events.get(i);
      String curr_host = (String) event.get("hostname");
      String other_host = (String) event.get("other_host");
      host_set.add(curr_host);
      host_set.add(other_host);
    }
    int num_hosts = host_set.size();

    Iterator<String> host_iter = host_set.iterator();
    for (int i = 0; i < num_hosts && host_iter.hasNext(); i++) {
      String curr_host = host_iter.next();
      host_indices.put(curr_host, new Integer(i));
      host_rev_indices.put(new Integer(i),curr_host);
    }

    System.out.println("Number of hosts: " + num_hosts);
    long stats[][] = new long[num_hosts][num_hosts];
    long count[][] = new long[num_hosts][num_hosts]; // used for averaging

    int start_millis = 0, end_millis = 0;

    // deliberate design choice to duplicate code PER possible operation
    // otherwise we have to do the mode check N times, for N states returned
    //
    // compute aggregate statistics
    log.info("Query statistic type: "+this.query_stat_type);
    if (this.query_stat_type.equals("transaction_count")) {
      for(int i=0;i<events.size();i++) {
        HashMap<String, Object> event = events.get(i);
        start=(Long)event.get("start_time");
        end=(Long)event.get("finish_time");
        start_millis = Integer.parseInt(((String)event.get("start_time_millis")));
        end_millis = Integer.parseInt(((String)event.get("finish_time_millis")));      
        String cell = (String) event.get("state_name");      
        String this_host = (String) event.get("hostname");
        String other_host = (String) event.get("other_host");
        int this_host_idx = host_indices.get(this_host).intValue();
        int other_host_idx = host_indices.get(other_host).intValue();

        // to, from
        stats[other_host_idx][this_host_idx] += 1;
      }
    } else if (this.query_stat_type.equals("avg_duration")) {
      for(int i=0;i<events.size();i++) {
        HashMap<String, Object> event = events.get(i);
        start=(Long)event.get("start_time");
        end=(Long)event.get("finish_time");
        start_millis = Integer.parseInt(((String)event.get("start_time_millis")));
        end_millis = Integer.parseInt(((String)event.get("finish_time_millis")));      
        String cell = (String) event.get("state_name");      
        String this_host = (String) event.get("hostname");
        String other_host = (String) event.get("other_host");
        int this_host_idx = host_indices.get(this_host).intValue();
        int other_host_idx = host_indices.get(other_host).intValue();

        long curr_val = end_millis - start_millis + ((end - start)*1000);

        // to, from
        stats[other_host_idx][this_host_idx] += curr_val;
        count[other_host_idx][this_host_idx] += 1;
      }    
      for (int i = 0; i < num_hosts; i++) {
        for (int j = 0; j < num_hosts; j++) {
          if (count[i][j] > 0) stats[i][j] = stats[i][j] / count[i][j];
        }
      }
    } else if (this.query_stat_type.equals("avg_volume")) {
      for(int i=0;i<events.size();i++) {
        HashMap<String, Object> event = events.get(i);
        start=(Long)event.get("start_time");
        end=(Long)event.get("finish_time");
        start_millis = Integer.parseInt(((String)event.get("start_time_millis")));
        end_millis = Integer.parseInt(((String)event.get("finish_time_millis")));      
        String cell = (String) event.get("state_name");      
        String this_host = (String) event.get("hostname");
        String other_host = (String) event.get("other_host");
        int this_host_idx = host_indices.get(this_host).intValue();
        int other_host_idx = host_indices.get(other_host).intValue();

        long curr_val = Long.parseLong((String)event.get("bytes"));

        // to, from
        stats[other_host_idx][this_host_idx] += curr_val;
        count[other_host_idx][this_host_idx] += 1;
      }    
      for (int i = 0; i < num_hosts; i++) {
        for (int j = 0; j < num_hosts; j++) {
          if (count[i][j] > 0) stats[i][j] = stats[i][j] / count[i][j];
        }
      }
    } else if (this.query_stat_type.equals("total_duration")) {
      for(int i=0;i<events.size();i++) {
        HashMap<String, Object> event = events.get(i);
        start=(Long)event.get("start_time");
        end=(Long)event.get("finish_time");
        start_millis = Integer.parseInt(((String)event.get("start_time_millis")));
        end_millis = Integer.parseInt(((String)event.get("finish_time_millis")));      
        String cell = (String) event.get("state_name");      
        String this_host = (String) event.get("hostname");
        String other_host = (String) event.get("other_host");
        int this_host_idx = host_indices.get(this_host).intValue();
        int other_host_idx = host_indices.get(other_host).intValue();

        double curr_val = end_millis - start_millis + ((end - start)*1000);

        // to, from
        stats[other_host_idx][this_host_idx] += curr_val;
      } 
    } else if (this.query_stat_type.equals("total_volume")) {
      for(int i=0;i<events.size();i++) {
        HashMap<String, Object> event = events.get(i);
        start=(Long)event.get("start_time");
        end=(Long)event.get("finish_time");
        start_millis = Integer.parseInt(((String)event.get("start_time_millis")));
        end_millis = Integer.parseInt(((String)event.get("finish_time_millis")));      
        String cell = (String) event.get("state_name");      
        String this_host = (String) event.get("hostname");
        String other_host = (String) event.get("other_host");
        int this_host_idx = host_indices.get(this_host).intValue();
        int other_host_idx = host_indices.get(other_host).intValue();

        long curr_val = Long.parseLong((String)event.get("bytes"));

        // to, from
        stats[other_host_idx][this_host_idx] += curr_val;
      }    
    }
    
    int [] permute = null;
    if (sort_nodes) {
      permute = hClust(stats);
      stats = doPermute(stats,permute);
    }
    
    Table agg_tab = new Table();
    agg_tab.addColumn("stat", long.class);
    min = Long.MAX_VALUE;
    max = Long.MIN_VALUE;
    agg_tab.addRows(num_hosts*num_hosts);
    
    // row-wise placement (row1, followed by row2, etc.)
    for (int i = 0; i < num_hosts; i++) {
      for (int j = 0; j < num_hosts; j++) {
        agg_tab.setLong((i*num_hosts)+j,"stat",stats[i][j]);
        if (stats[i][j] > max) max = stats[i][j];
        if (stats[i][j] > 0 && stats[i][j] < min) min = stats[i][j];
      }
    }
    if (min == Long.MAX_VALUE) min = 0;
    
    log.info(agg_tab);
    
    // collate data
    HeatmapData hd = new HeatmapData();
    hd.stats = new long[num_hosts][num_hosts];
    hd.stats = stats;
    hd.min = min;
    hd.max = max;
    hd.num_hosts = num_hosts;
    hd.agg_tab = agg_tab;
    
    this.add_info_extra = new String("\nState: "+this.prettyStateNames.get(this.query_state)+
      " ("+events.size()+" "+this.query_state+"'s ["+this.query_stat_type+"])\n" + 
      "Plotted value range: ["+hd.min+","+hd.max+"] (Zeros in black)");

    hd.hostnames = new String [num_hosts];
    for (int i = 0; i < num_hosts; i++) {
      String curr_host = host_rev_indices.get(new Integer(permute[i]));
      if (sort_nodes) {
        hd.hostnames[i] = new String(curr_host);
      } else {
        hd.hostnames[i] = new String(curr_host);
      }
    }
    
    return hd;
  }
  
}