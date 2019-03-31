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
import prefuse.util.collections.*;
import prefuse.util.*;
import prefuse.*;

import org.apache.hadoop.chukwa.hicc.OfflineTimeHandler;
import org.apache.hadoop.chukwa.hicc.TimeHandler;
import org.apache.hadoop.chukwa.util.DatabaseWriter;
import org.apache.hadoop.chukwa.database.Macro;
import org.apache.hadoop.chukwa.util.XssFilter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.http.*;

import java.sql.*;
import java.util.*;

import java.awt.Font;
import java.awt.geom.Rectangle2D;

/**
 * Static image generation for Swimlanes visualization for scalable 
 * rendering on front-end client (web-browser)
 * Handles database data retrieval, transforming data to form for 
 * visualization elements, and initializing and calling visualization
 * elements
 */
public class Swimlanes {

  private static Log log = LogFactory.getLog(Swimlanes.class);

  int SIZE_X=1600, SIZE_Y=1600;
  final int [] BORDER = {50,50,50,50};
  final int LEGEND_X_OFFSET = 50;
  final int LEGEND_Y_OFFSET = 25;
  final int LEGEND_TEXT_OFFSET = 20;
  final int LEGEND_FONT_SIZE = 18;
  final int AXIS_NAME_FONT_SIZE = 24;

  protected boolean offline_use = true;
  protected HttpServletRequest request;
  
  protected String abc;

  /**
   * Modifier for generic Swimlanes plots to plot shuffle, sort, and reducer
   * states of same reduce on same line 
   */
  protected static class MapReduceSwimlanes {
    protected Table plot_tab;
    protected HashMap<String, ArrayList<Tuple> > reducepart_hash;
    protected boolean collate_reduces = false;
    
    public MapReduceSwimlanes() {
      this.plot_tab = new Table();
      this.plot_tab.addColumn("ycoord",float.class);
      this.plot_tab.addColumn("state_name",String.class);
      this.plot_tab.addColumn("hostname",String.class);
      this.plot_tab.addColumn("friendly_id",String.class);
      this.plot_tab.addColumn(START_FIELD_NAME,double.class);
      this.plot_tab.addColumn(END_FIELD_NAME,double.class);
      this.plot_tab.addColumn(PolygonRenderer.POLYGON,float[].class);
      this.reducepart_hash = new HashMap<String, ArrayList<Tuple> >();
    }
    
    public void populateTable_OneLinePerState(Table orig_tab) {
      IntIterator rownumiter;
      int newrownum, origrownum;
      rownumiter = orig_tab.rows(); // iterate over everything
      while (rownumiter.hasNext()) {
        origrownum = ((Integer)rownumiter.next()).intValue();
        newrownum = this.plot_tab.addRow();
        this.plot_tab.set(newrownum, "state_name", orig_tab.getString(origrownum, "state_name"));
        this.plot_tab.set(newrownum, "ycoord", orig_tab.getInt(origrownum, "seqno"));
        this.plot_tab.set(newrownum,"hostname",orig_tab.getString(origrownum,"hostname"));
        this.plot_tab.set(newrownum,"friendly_id",orig_tab.getString(origrownum,"friendly_id"));
        this.plot_tab.set(newrownum,START_FIELD_NAME, orig_tab.getDouble(origrownum,START_FIELD_NAME));
        this.plot_tab.set(newrownum,END_FIELD_NAME, orig_tab.getDouble(origrownum,END_FIELD_NAME));
      }      
    }
    
    public void populateTable_CollateReduces(Table orig_tab) {
      IntIterator rownumiter;
      int newrownum, origrownum;
      
      this.collate_reduces = true;
      
      // add maps normally
      rownumiter = orig_tab.rows(
        (Predicate) ExpressionParser.parse("[state_name] == 'map' " + 
          "OR [state_name] == 'shuffle_local' " + 
          "OR [state_name] == 'shuffle_remote'")
      );
      
      while (rownumiter.hasNext()) {
        origrownum = ((Integer)rownumiter.next()).intValue();
        newrownum = this.plot_tab.addRow();
        this.plot_tab.set(newrownum, "state_name", orig_tab.getString(origrownum, "state_name"));
        this.plot_tab.set(newrownum, "ycoord", orig_tab.getInt(origrownum, "seqno"));
        this.plot_tab.set(newrownum,"hostname",orig_tab.getString(origrownum,"hostname"));
        this.plot_tab.set(newrownum,"friendly_id",orig_tab.getString(origrownum,"friendly_id"));
        this.plot_tab.set(newrownum,START_FIELD_NAME, orig_tab.getDouble(origrownum,START_FIELD_NAME));
        this.plot_tab.set(newrownum,END_FIELD_NAME, orig_tab.getDouble(origrownum,END_FIELD_NAME));
      }

      // special breakdown for reduces
      IntIterator rownumiter3 = orig_tab.rows(
        (Predicate) ExpressionParser.parse("[state_name] == 'reduce_reducer' " +
          "OR [state_name] == 'reduce_shufflewait' " + 
          "OR [state_name] == 'reduce_sort' " + 
          "OR [state_name] == 'reduce'")
      );
      
      ArrayList<Tuple> tuple_array;
      while (rownumiter3.hasNext()) {
        origrownum = ((Integer)rownumiter3.next()).intValue();
        if (orig_tab.getString(origrownum,"state_name").equals("reduce")) {
          continue; // do NOT add reduces
        }
        String curr_reduce = orig_tab.getString(origrownum, "friendly_id");
        newrownum = this.plot_tab.addRow();
        
        this.plot_tab.set(newrownum, "state_name", orig_tab.getString(origrownum, "state_name"));
        this.plot_tab.set(newrownum, "ycoord", orig_tab.getInt(origrownum, "seqno"));
        this.plot_tab.set(newrownum,"hostname",orig_tab.getString(origrownum,"hostname"));
        this.plot_tab.set(newrownum,"friendly_id",orig_tab.getString(origrownum,"friendly_id"));        
        this.plot_tab.set(newrownum,START_FIELD_NAME, orig_tab.getDouble(origrownum,START_FIELD_NAME));
        this.plot_tab.set(newrownum,END_FIELD_NAME, orig_tab.getDouble(origrownum,END_FIELD_NAME));
        
        tuple_array = this.reducepart_hash.get(curr_reduce);
        if (tuple_array == null) {
          tuple_array = new ArrayList<Tuple>();
          tuple_array.add(this.plot_tab.getTuple(newrownum));
          this.reducepart_hash.put(curr_reduce, tuple_array);
        } else {
          tuple_array.add(this.plot_tab.getTuple(newrownum));
        }
      }  
    }
    
    public void populateTable_MapsReducesOnly(Table orig_tab) {
      IntIterator rownumiter;
      int newrownum, origrownum;
      rownumiter = orig_tab.rows(
        (Predicate) ExpressionParser.parse("[state_name] == 'map' OR [state_name] == 'reduce'")
      );
      while (rownumiter.hasNext()) {
        origrownum = ((Integer)rownumiter.next()).intValue();
        newrownum = this.plot_tab.addRow();
        this.plot_tab.set(newrownum, "state_name", orig_tab.getString(origrownum, "state_name"));
        this.plot_tab.set(newrownum, "ycoord", orig_tab.getInt(origrownum, "seqno"));
        this.plot_tab.set(newrownum,"hostname",orig_tab.getString(origrownum,"hostname"));
        this.plot_tab.set(newrownum,"friendly_id",orig_tab.getString(origrownum,"friendly_id"));        
        this.plot_tab.set(newrownum,START_FIELD_NAME, orig_tab.getDouble(origrownum,START_FIELD_NAME));
        this.plot_tab.set(newrownum,START_FIELD_NAME, orig_tab.getDouble(origrownum,END_FIELD_NAME));
      }
    }
    
    /**
     * Reassigns Y coord values to group by state
     */
    public void groupByState() {
      int counter, rownum;
      int rowcount = this.plot_tab.getRowCount();
      HashSet<String> states = new HashSet<String>();
      String curr_state = null;
      Iterator<String> state_iter;
      IntIterator rownumiter;
      
      for (int i = 0; i < rowcount; i++) {
        states.add(this.plot_tab.getString(i,"state_name"));
      }
     
      state_iter = states.iterator();
      counter = 1;
      while (state_iter.hasNext()) {
        curr_state = state_iter.next();
        
        if (this.collate_reduces && ((curr_state.equals("reduce_reducer") || curr_state.equals("reduce_sort")))) {
          continue;
        }
        rownumiter = this.plot_tab.rows(
          (Predicate) ExpressionParser.parse("[state_name] == '"+curr_state+"'")
        );
        if (this.collate_reduces && curr_state.equals("reduce_shufflewait")) {
          while (rownumiter.hasNext()) {
            rownum = ((Integer)rownumiter.next()).intValue();
            this.plot_tab.setFloat(rownum,"ycoord",(float)counter);
            
            ArrayList<Tuple> alt = this.reducepart_hash.get(this.plot_tab.getString(rownum,"friendly_id"));
            Object [] tarr = alt.toArray();
            for (int i = 0; i < tarr.length; i++) ((Tuple)tarr[i]).setFloat("ycoord",(float)counter);
            counter++;            
          }
        } else {
          while (rownumiter.hasNext()) {
            rownum = ((Integer)rownumiter.next()).intValue();
            this.plot_tab.setFloat(rownum,"ycoord",(float)counter);
            counter++;
          }          
        }
      }
    }
    
    public void groupByStartTime() {
      int counter, rownum;
      String curr_state = null;
      IntIterator rownumiter;
     
      rownumiter = this.plot_tab.rowsSortedBy(START_FIELD_NAME, true);
     
      counter = 1;
      while (rownumiter.hasNext()) {
        rownum = ((Integer)rownumiter.next()).intValue();
        curr_state = this.plot_tab.getString(rownum, "state_name");        

        if (this.collate_reduces && curr_state.equals("reduce_shufflewait")) {
          this.plot_tab.setFloat(rownum,"ycoord",(float)counter);
          ArrayList<Tuple> alt = this.reducepart_hash.get(this.plot_tab.getString(rownum,"friendly_id"));
          Object [] tarr = alt.toArray();
          for (int i = 0; i < tarr.length; i++) ((Tuple)tarr[i]).setFloat("ycoord",(float)counter);
          counter++;   
        } else if (!curr_state.equals("reduce_sort") && !curr_state.equals("reduce_reducer")) {
          this.plot_tab.setFloat(rownum,"ycoord",(float)counter);          
          counter++;
        }
      }
    }
    
    public void groupByEndTime() {
      int counter, rownum;
      String curr_state = null;
      IntIterator rownumiter;
     
      rownumiter = this.plot_tab.rowsSortedBy(END_FIELD_NAME, true);
      counter = 1;
      while (rownumiter.hasNext()) {
        rownum = ((Integer)rownumiter.next()).intValue();
        curr_state = this.plot_tab.getString(rownum, "state_name");        

        if (this.collate_reduces && curr_state.equals("reduce_reducer")) {
          this.plot_tab.setFloat(rownum,"ycoord",(float)counter);
          ArrayList<Tuple> alt = this.reducepart_hash.get(this.plot_tab.getString(rownum,"friendly_id"));
          Object [] tarr = alt.toArray();
          for (int i = 0; i < tarr.length; i++) ((Tuple)tarr[i]).setFloat("ycoord",(float)counter);
          counter++;
        } else if (!curr_state.equals("reduce_sort") && !curr_state.equals("reduce_shufflewait")) {
          this.plot_tab.setFloat(rownum,"ycoord",(float)counter);          
          counter++;
        }
      }
    }    
    
    public VisualTable addToVisualization(Visualization viz, String groupname) {
      return viz.addTable(groupname, this.plot_tab);
    }
  }

  /**
   * Provide constant mapping between state names and colours
   * so that even if particular states are missing, the colours are fixed
   * for each state
   */
  public static class SwimlanesStatePalette {
    protected final String [] states = {"map","reduce","reduce_shufflewait","reduce_sort","reduce_reducer","shuffle"};
    HashMap<String,Integer> colourmap; 
    protected int [] palette;
    public SwimlanesStatePalette() {
      palette = ColorLib.getCategoryPalette(states.length);
      colourmap = new HashMap<String,Integer>();
      for (int i = 0; i < states.length; i++) {
        colourmap.put(states[i], Integer.valueOf(palette[i]));
      }
    }
    public int getColour(String state_name) {
      Integer val = colourmap.get(state_name);
      if (val == null) {
        return ColorLib.color(java.awt.Color.BLACK);
      } else {
        return val.intValue();
      }
    }
    public int getNumStates() {
      return this.states.length;
    }
    public String [] getStates() {
      return this.states.clone();
    }
  }

  /**
   * Provides convenient rescaling of raw values to be plotted to
   * actual pixels for plotting on image
   */
  public static class CoordScaler {
    double x_pixel_size, y_pixel_size;
    double x_max_value, y_max_value, x_min_value, y_min_value;
    double x_start, y_start;
    
    public CoordScaler() {
      this.x_pixel_size = 0.0;
      this.y_pixel_size = 0.0;
      this.x_max_value = 1.0;
      this.y_max_value = 1.0;
      this.x_min_value = 0.0;
      this.y_min_value = 0.0;
      this.x_start = 0.0;
      this.y_start = 0.0;
    }
    public void set_pixel_start(double x, double y) {
      this.x_start = x;
      this.y_start = y;
    }
    public void set_pixel_size(double x, double y) {
      this.x_pixel_size = x;
      this.y_pixel_size = y;
    }
    public void set_value_ranges(double x_min, double y_min, double x_max, double y_max) {
      this.x_max_value = x_max;
      this.y_max_value = y_max;
      this.x_min_value = x_min;
      this.y_min_value = y_min;
    }
    public double get_x_coord(double x_value) {
      return x_start+(((x_value - x_min_value) / (x_max_value-x_min_value)) * x_pixel_size);
    }
    public double get_y_coord(double y_value) {
      // this does "inverting" to shift the (0,0) point from top-right to bottom-right
      return y_start+(y_pixel_size - ((((y_value - y_min_value) / (y_max_value-y_min_value)) * y_pixel_size)));
    }
  }

  /**
   * Prefuse action for plotting a line for each state
   */
  public static class SwimlanesStateAction extends GroupAction {
    
    protected CoordScaler cs;
    
    public SwimlanesStateAction() {
      super();
    }
    
    public SwimlanesStateAction(String group, CoordScaler cs) {
      super(group);
      this.cs = cs;
    }
    
    public void run (double frac) {
      VisualItem item = null;
      SwimlanesStatePalette pal = new SwimlanesStatePalette();
      
      Iterator<?> curr_group_items = this.m_vis.items(this.m_group);
          
      while (curr_group_items.hasNext()) {
        item = (VisualItem) curr_group_items.next();
        
        double start_time = item.getDouble(START_FIELD_NAME);
        double finish_time = item.getDouble(END_FIELD_NAME);        
        item.setShape(Constants.POLY_TYPE_LINE);
        item.setX(0.0);
        item.setY(0.0);        
        
        float [] coords = new float[4];
        coords[0] = (float) cs.get_x_coord(start_time);
        coords[1] = (float) cs.get_y_coord((double)item.getInt("ycoord"));
        coords[2] = (float) cs.get_x_coord(finish_time);
        coords[3] = (float) cs.get_y_coord((double)item.getInt("ycoord"));

        item.set(VisualItem.POLYGON,coords);
        item.setStrokeColor(pal.getColour(item.getString("state_name")));
      }
    }    
  } // SwimlanesStateAction

  // keys that need to be filled:
  // period (last1/2/3/6/12/24hr,last7d,last30d), time_type (range/last), start, end
  protected HashMap<String, String> param_map;
  
  protected String cluster;
  protected String timezone;
  protected String shuffle_option;
  protected final String table = "mapreduce_fsm";
  protected boolean plot_legend = true;
  protected String jobname = null;
  
  protected Display dis;
  protected Visualization viz;
  
  protected Rectangle2D dataBound = new Rectangle2D.Double();
  protected Rectangle2D xlabBound = new Rectangle2D.Double();
  protected Rectangle2D ylabBound = new Rectangle2D.Double();
  protected Rectangle2D labelBottomBound = new Rectangle2D.Double();
  
  static final String START_FIELD_NAME = "start_time_num";
  static final String END_FIELD_NAME = "finish_time_num";
  
  /* Different group names allow control of what Renderers to use */
  final String maingroup = "Job";
  final String othergroup = "Misc";
  final String labelgroup = "Label";
  final String legendgroup = "Legend";
  final String legendshapegroup = "LegendShape";
  
  public Swimlanes() {
    this.cluster = "";
    this.timezone = "";
    this.shuffle_option = "";
    param_map = new HashMap<String, String>();
  }
  
  /**
   * Constructor for Swimlanes visualization object
   * @param timezone Timezone string from environment
   * @param cluster Cluster name from environment
   * @param event_type Whether to display shuffles or not
   * @param valmap HashMap of key/value pairs simulating parameters from a HttpRequest
   */
  public Swimlanes
    (String timezone, String cluster, String event_type, 
     HashMap<String, String> valmap) 
  {
    this.cluster = cluster;
    if (timezone != null) {
      this.timezone = timezone;
    } else {
      this.timezone = null;
    }
    this.shuffle_option = event_type;
    
    /* This should "simulate" an HttpServletRequest
     * Need to have "start" and "end" in seconds since Epoch
     */
    this.param_map = valmap;
  }
  
  public Swimlanes
    (String timezone, String cluster, String event_type, 
     HashMap<String, String> valmap, int width, int height) 
  {
    this.cluster = cluster;
    if (timezone != null) {
      this.timezone = timezone;
    } else {
      this.timezone = null;
    }
    this.shuffle_option = event_type;
    
    /* This should "simulate" an HttpServletRequest
     * Need to have "start" and "end" in seconds since Epoch
     */
    this.param_map = valmap; 
    
    this.SIZE_X = width;
    this.SIZE_Y = height;
  }
  
  public Swimlanes
    (String timezone, String cluster, String event_type, 
     HashMap<String, String> valmap, int width, int height,
     String legend_opt) 
  {
    this.cluster = cluster;
    if (timezone != null) {
      this.timezone = timezone;
    } else {
      this.timezone = null;
    }
    this.shuffle_option = event_type;
    
    /* This should "simulate" an HttpServletRequest
     * Need to have "start" and "end" in seconds since Epoch
     */
    this.param_map = valmap;
    
    this.SIZE_X = width;
    this.SIZE_Y = height;
    
    if (legend_opt.equals("nolegend")) {
      this.plot_legend = false;
    }
    
  }
  
  public Swimlanes(HttpServletRequest request) {
    XssFilter xf = new XssFilter(request);
    this.offline_use = false;
    this.request = request;
    HttpSession session = request.getSession();
    this.cluster = session.getAttribute("cluster").toString();
      String evt_type = xf.getParameter("event_type");
    if (evt_type != null) {
      this.shuffle_option = evt_type;
    } else {
      this.shuffle_option = "noshuffle";
    }
    this.timezone = session.getAttribute("time_zone").toString();
  }
  
  /**
   * Set job ID to filter results on
   * Call before calling @see #run
   * @param s job name
   */
  public void setJobName(String s) {
    this.jobname = s;
  }

  /**
   * Set dimensions of image to be generated
   * Call before calling @see #run
   * @param width image width in pixels
   * @param height image height in pixels
   */  
  public void setDimensions(int width, int height) {
    this.SIZE_X=width;
    this.SIZE_Y=height;
  }
  
  /**
   * Specify whether to print legend of states
   * Advisable to not print legend for excessively small images since
   * legend has fixed point size
   * Call before calling @see #run
   * @param legendopt parameter to turn on legends
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
   * @param output output stream of image
   * @param img_fmt image format
   * @param scale image scaling factor
   * @return true if image is saved
   */
  public boolean getImage(java.io.OutputStream output, String img_fmt, double scale) {
    dis = new Display(this.viz);
    dis.setSize(SIZE_X,SIZE_Y);
    dis.setHighQuality(true);
    dis.setFont(new Font(Font.SANS_SERIF,Font.PLAIN,24));
    return dis.saveImage(output, img_fmt, scale);
  } 
  
  /**
   * Adds a column to given table by converting timestamp to long with
   * seconds since epoch, and adding milliseconds from additional column
   * in original table
   *
   * @param origTable Table to add to
   * @param srcFieldName Name of column containing timestamp
   * @param srcMillisecondFieldName Name of column containing millisecond value of time 
   * @param dstFieldName Name of new column to add
   * 
   * @return Modified table with added column
   */
  protected Table addTimeCol 
    (Table origTable, String srcFieldName, 
     String srcMillisecondFieldName, String dstFieldName)
  {
    origTable.addColumn(dstFieldName, long.class);
    
    int total_rows = origTable.getRowCount();
    for (int curr_row_num = 0; curr_row_num < total_rows; curr_row_num++) {
      origTable.setLong(curr_row_num, dstFieldName, 
        ((Timestamp)origTable.get(curr_row_num, srcFieldName)).getTime() + 
        origTable.getLong(curr_row_num, srcMillisecondFieldName)
      );
    }
    
    return origTable;
  }
  
  /**
   * Adds a column with number of seconds of timestamp elapsed since lowest
   * start time; allows times to be plotted as a delta of the start time
   * 
   * @param origTable Table to add column to
   * @param srcFieldName Name of column containing timestamp
   * @param srcMillisecondFieldName Name of column containing millisecond value of time 
   * @param dstFieldName Name of new column to add
   *   
   * @return Modified table with added column
   */
  protected Table addTimeOffsetCol
    (Table origTable, String srcFieldName,
     String srcMillisecondFieldName, String dstFieldName,
     long timeOffset) 
  {
    Table newtable = addTimeCol(origTable, srcFieldName, 
      srcMillisecondFieldName, dstFieldName + "_fulltime");
      
    ColumnMetadata dstcol = newtable.getMetadata(dstFieldName + "_fulltime");
    long mintime = newtable.getLong(dstcol.getMinimumRow(), dstFieldName + "_fulltime");
    
    if (timeOffset == 0) {
      newtable.addColumn(dstFieldName, "ROUND((["+dstFieldName+"_fulltime] - " + mintime +"L) / 1000L)");
    } else {
      newtable.addColumn(dstFieldName, "ROUND((["+dstFieldName+"_fulltime] - " + timeOffset +"L) / 1000L)");      
    }
    
    return newtable;
  }
  
  protected void setupRenderer() {
    this.viz.setRendererFactory(new RendererFactory(){
      AbstractShapeRenderer sr = new ShapeRenderer();
      ShapeRenderer sr_big = new ShapeRenderer(20);
      Renderer arY = new AxisRenderer(Constants.LEFT, Constants.TOP);
      Renderer arX = new AxisRenderer(Constants.CENTER, Constants.BOTTOM);
      PolygonRenderer pr = new PolygonRenderer(Constants.POLY_TYPE_LINE);
      LabelRenderer lr = new LabelRenderer("label");
      LabelRenderer lr_legend = new LabelRenderer("label");
      
      public Renderer getRenderer(VisualItem item) {
        lr.setHorizontalAlignment(Constants.CENTER);
        lr.setVerticalAlignment(Constants.TOP);
        lr_legend.setHorizontalAlignment(Constants.LEFT);
        lr_legend.setVerticalAlignment(Constants.CENTER);
        
        if (item.isInGroup("ylab")) {
          return arY;
        } else if (item.isInGroup("xlab")) {
          return arX;
        } else if (item.isInGroup(maingroup)) {
          return pr;
        } else if (item.isInGroup(labelgroup)) {
          return lr;
        } else if (item.isInGroup(legendgroup)) {
          return lr_legend;
        } else if (item.isInGroup(legendshapegroup)) {
          return sr_big;
        } else {
          return sr;
        }
      }
    });
  }
  
  // setup columns: add additional time fields
  protected Table setupDataTable() {
    Table res_tab = this.getData();    
    if (res_tab == null) {
        return res_tab;
    }
    
    res_tab.addColumn("seqno","ROW()");
    res_tab = addTimeOffsetCol(res_tab, "start_time", "start_time_millis", START_FIELD_NAME, 0);    
    ColumnMetadata dstcol = res_tab.getMetadata(START_FIELD_NAME);
    long mintime = ((Timestamp)res_tab.get(dstcol.getMinimumRow(), "start_time")).getTime();
    res_tab = addTimeOffsetCol(res_tab, "finish_time", "finish_time_millis", END_FIELD_NAME, mintime);    
    res_tab.addColumn(PolygonRenderer.POLYGON,float[].class);
    
    log.debug("After adding seqno: #cols: " + res_tab.getColumnCount() + "; #rows: " + res_tab.getRowCount());
    
    return res_tab;
  }
  
  protected void addAxisNames() {
    Table textlabels_table = new Table();
    textlabels_table.addColumn("label",String.class);
    textlabels_table.addColumn("type",String.class);
    textlabels_table.addRow();
    textlabels_table.setString(0,"label","Time/s");
    textlabels_table.setString(0,"type","xaxisname");
    
    VisualTable textlabelsviz = this.viz.addTable(labelgroup, textlabels_table);
    textlabelsviz.setX(0,SIZE_X/2d);
    textlabelsviz.setY(0,SIZE_Y - BORDER[2] + (BORDER[2]*0.1));
    textlabelsviz.setTextColor(0,ColorLib.color(java.awt.Color.GRAY));
    textlabelsviz.setFont(0,new Font(Font.SANS_SERIF,Font.PLAIN,AXIS_NAME_FONT_SIZE));
  }
  
  protected void addLegend() {
    SwimlanesStatePalette ssp = new SwimlanesStatePalette();
    
    Table shapes_table = new Table();
    shapes_table.addColumn(VisualItem.X,float.class);
    shapes_table.addColumn(VisualItem.Y,float.class);
    
    Table legend_labels_table = new Table();
    legend_labels_table.addColumn("label",String.class);
    
    // add labels
    int num_states = ssp.getNumStates();
    String [] state_names = ssp.getStates();
    legend_labels_table.addRows(num_states);
    shapes_table.addRows(num_states);
    for (int i = 0; i < num_states; i++) {
      legend_labels_table.setString(i,"label",state_names[i]);
    }
    
    // add legend shapes, manipulate visualitems to set colours
    VisualTable shapes_table_viz = viz.addTable(legendshapegroup, shapes_table);
    float start_x = BORDER[0] + LEGEND_X_OFFSET;
    float start_y = BORDER[1] + LEGEND_Y_OFFSET;
    float incr = (float) 30.0;
    for (int i = 0; i < num_states; i++) {
      shapes_table_viz.setFillColor(i, ssp.getColour(state_names[i]));
      shapes_table_viz.setFloat(i, VisualItem.X, start_x);
      shapes_table_viz.setFloat(i, VisualItem.Y, start_y + (i * incr));
    }
    
    // add legend labels, manipulate visualitems to set font
    VisualTable legend_labels_table_viz = this.viz.addTable(legendgroup, legend_labels_table);
    for (int i = 0; i < num_states; i++) {
      legend_labels_table_viz.setFloat(i, VisualItem.X, start_x + LEGEND_TEXT_OFFSET);
      legend_labels_table_viz.setFloat(i, VisualItem.Y, start_y + (i * incr));
      legend_labels_table_viz.setTextColor(i,ColorLib.color(java.awt.Color.BLACK));
      legend_labels_table_viz.setFont(i,new Font(Font.SANS_SERIF,Font.PLAIN,LEGEND_FONT_SIZE));
    }
    
  }
  
  public void run() {

    // setup bounds
    this.dataBound.setRect(BORDER[0],BORDER[1],SIZE_X-BORDER[2]-BORDER[0],SIZE_Y-BORDER[3]-BORDER[1]);
    this.xlabBound.setRect(BORDER[0],BORDER[1],SIZE_X-BORDER[2]-BORDER[0],SIZE_Y-BORDER[3]-BORDER[1]);
    this.ylabBound.setRect(BORDER[0],BORDER[1],SIZE_X-BORDER[2]-BORDER[0],SIZE_Y-BORDER[3]-BORDER[1]);
    this.labelBottomBound.setRect(BORDER[0],SIZE_X-BORDER[2],SIZE_Y-BORDER[0]-BORDER[1],BORDER[3]);
    
    // setup visualization
    this.viz = new Visualization();
    this.setupRenderer();
    
    // add table to visualization
    Table raw_data_tab = this.setupDataTable();
    MapReduceSwimlanes mrs = new MapReduceSwimlanes();
    mrs.populateTable_CollateReduces(raw_data_tab);
    mrs.groupByState();
    VisualTable maindatatable = mrs.addToVisualization(this.viz, maingroup);
        
    addAxisNames();
    if (plot_legend) {
      addLegend();
    }

    // plot swimlanes lines: setup axes, call custom action
    ActionList draw = new ActionList();
    {
      // setup axes
      AxisLayout xaxis = new AxisLayout(maingroup, START_FIELD_NAME, Constants.X_AXIS, VisiblePredicate.TRUE);
      AxisLayout yaxis = new AxisLayout(maingroup, "ycoord", Constants.Y_AXIS, VisiblePredicate.FALSE);    
      xaxis.setLayoutBounds(dataBound);
      yaxis.setLayoutBounds(dataBound);
    
      ColumnMetadata starttime_meta = maindatatable.getMetadata(START_FIELD_NAME);    
      ColumnMetadata finishtime_meta = maindatatable.getMetadata(END_FIELD_NAME);    
      ColumnMetadata ycoord_meta = maindatatable.getMetadata("ycoord");
      long x_min = (long) ((Double)maindatatable.get(starttime_meta.getMinimumRow(), START_FIELD_NAME)).doubleValue();
      long x_max = (long) ((Double)maindatatable.get(finishtime_meta.getMaximumRow(), END_FIELD_NAME)).doubleValue();
      xaxis.setRangeModel(new NumberRangeModel(x_min,x_max,x_min,x_max));
      float y_max = maindatatable.getFloat(ycoord_meta.getMaximumRow(),"ycoord");
      yaxis.setRangeModel(new NumberRangeModel(0,y_max,0,y_max));
      
      // call custom action to plot actual swimlanes lines
      CoordScaler cs = new CoordScaler();
      cs.set_pixel_size(SIZE_X-BORDER[0]-BORDER[2], SIZE_Y-BORDER[1]-BORDER[3]);
      cs.set_pixel_start(BORDER[0],BORDER[1]);
      cs.set_value_ranges(x_min,0,x_max,y_max);
      //SwimlanesStateAction swimlaneslines = new SwimlanesStateAction(maingroup, cs);
      SwimlanesStateAction swimlaneslines = new SwimlanesStateAction(maingroup, cs);
      
      // add everything to the plot
      draw.add(xaxis);
      draw.add(yaxis);
      draw.add(swimlaneslines);
      
      AxisLabelLayout xlabels = new AxisLabelLayout("xlab", xaxis, xlabBound);
      this.viz.putAction("xlabels",xlabels);
      AxisLabelLayout ylabels = new AxisLabelLayout("ylab", yaxis, ylabBound);
      this.viz.putAction("ylabels",ylabels);
    }
    
    // add axes names
    {
      SpecifiedLayout sl = new SpecifiedLayout(labelgroup, VisualItem.X, VisualItem.Y);
      ActionList labeldraw = new ActionList();
      labeldraw.add(sl);
      this.viz.putAction(labelgroup, labeldraw);
    }
    
    // add legend
    if (plot_legend) {
      ShapeAction legend_sa = new ShapeAction(legendshapegroup);
      SpecifiedLayout legendlabels_sl = new SpecifiedLayout(legendgroup, VisualItem.X, VisualItem.Y);
    
      ActionList legenddraw = new ActionList();
      legenddraw.add(legend_sa);
      this.viz.putAction(legendshapegroup, legenddraw);
      ActionList legendlabelsdraw = new ActionList();
      legendlabelsdraw.add(legendlabels_sl);
      this.viz.putAction(legendgroup,legendlabelsdraw);
    }
    
    // draw everything else
    this.viz.putAction("draw",draw);

    // finally draw
    this.viz.run("draw");
    this.viz.run("xlabels");
    this.viz.run("ylabels");

  }
  
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value =
      "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE", 
      justification = "Dynamic based upon tables in the database")
  public Table getData() {
    // preliminary setup
    OfflineTimeHandler time_offline;
    TimeHandler time_online;
    long start, end;
    
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
    String query;
    
    // setup query
    if (this.shuffle_option != null && this.shuffle_option.equals("shuffles")) {
      query = "select job_id,friendly_id,start_time,finish_time,start_time_millis,finish_time_millis,status,state_name,hostname from ["+this.table+"] where finish_time between '[start]' and '[end]'";
    } else {
      query = "select job_id,friendly_id,start_time,finish_time,start_time_millis,finish_time_millis,status,state_name,hostname from ["+this.table+"] where finish_time between '[start]' and '[end]' and not state_name like 'shuffle_local' and not state_name like 'shuffle_remote'";
    }
    if (this.jobname != null) {
      query = query + " and job_id like '"+ this.jobname +"'";
    }
    Macro mp = new Macro(start,end,query);
    query = mp.toString() + " order by start_time";
    
    Table rs_tab = null;    
    DatabaseDataSource dds; 

    log.debug("Query: " + query);
    // execute query
    try {
      dds = ConnectionFactory.getDatabaseConnection(dbw.getConnection());
      rs_tab = dds.getData(query);
    } catch (prefuse.data.io.DataIOException e) {
      System.err.println("prefuse data IO error: " + e);
      log.warn("prefuse data IO error: " + e);
      return null;
    } catch (SQLException e) {
      System.err.println("Error in SQL: " + e + " in statement: " + query);
      log.warn("Error in SQL: " + e + " in statement: " + query);
      return null;
    }
    
    HashMap<String, Integer> state_counts = new HashMap<String, Integer>();
    for (int i = 0; i < rs_tab.getRowCount(); i++) {
      String curr_state = rs_tab.getString(i, "state_name");
      Integer cnt = state_counts.get(curr_state);
      if (cnt == null) {
        state_counts.put(curr_state, Integer.valueOf(1));
      } else {
        state_counts.remove(curr_state);
        state_counts.put(curr_state, Integer.valueOf(cnt.intValue()+1));
      }
    }
    
    log.info("Search complete: #cols: " + rs_tab.getColumnCount() + "; #rows: " + rs_tab.getRowCount());
    
    return rs_tab;
  }
  
}
