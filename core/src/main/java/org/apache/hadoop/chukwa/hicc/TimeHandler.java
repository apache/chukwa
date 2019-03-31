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

package org.apache.hadoop.chukwa.hicc;


import javax.servlet.http.*;

import org.apache.hadoop.chukwa.util.XssFilter;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.StringTokenizer;
import java.text.SimpleDateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.mdimension.jchronic.Chronic;
import com.mdimension.jchronic.Options;
import com.mdimension.jchronic.utils.Span;

public class TimeHandler {
  private HttpSession session = null;
  private TimeZone tz = null;
  private long start = 0;
  private long end = 0;
  private String startDate = null;
  private String startHour = null;
  private String startMin = null;
  private String endDate = null;
  private String endHour = null;
  private String endMin = null;
  private String startS = null;
  private String endS = null;
  private XssFilter xf = null;
    private static Log log=LogFactory.getLog(TimeHandler.class);
  
  public TimeHandler(HttpServletRequest request) {
    this.tz = TimeZone.getTimeZone("UTC");
    init(request);
  }

  public TimeHandler(HttpServletRequest request, String tz) {
    if (tz != null) {
      this.tz = TimeZone.getTimeZone(tz);
    } else {
      this.tz = TimeZone.getTimeZone("UTC");
    }
    init(request);
  }

    /*
     * Using the Chronic library to parse the english string
     * and convert it to a long (millis seconds since 1970)
     */
    public long parseDateShorthand(String d) {
	Calendar now = Calendar.getInstance();
	long l=now.getTimeInMillis();
	d=d.trim();
	if (d.compareToIgnoreCase("now")!=0) {
	    Options options= new Options(false);
	    options.setCompatibilityMode(true);
	    options.setNow(now);
	    try {
		Span span = Chronic.parse(d, options);
		l = span.getBegin()*1000;
	    } catch (Exception e) {
		// exception when parsing
		log.error("parse error for: "+d);		
	    }
	}

	/*
	 * debug 
	 */
	/*
        SimpleDateFormat sf =
            new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	Date ld=new Date(l);

	log.error("Convert:"+d+" to "+Long.toString(l)+" - "+sf.format(ld)+ "-"+ld.getTime());
	*/

	return l;
    }

    public void parsePeriodValue(String period) {
	Calendar now = Calendar.getInstance();
	this.start = now.getTimeInMillis();
	this.end = now.getTimeInMillis();
	if (period.equals("last1hr")) {
	    start = end - (60 * 60 * 1000);
	} else if (period.equals("last2hr")) {
	    start = end - (2 * 60 * 60 * 1000);
	} else if (period.equals("last3hr")) {
	    start = end - (3 * 60 * 60 * 1000);
	} else if (period.equals("last6hr")) {
	    start = end - (6 * 60 * 60 * 1000);
	} else if (period.equals("last12hr")) {
	    start = end - (12 * 60 * 60 * 1000);
	} else if (period.equals("last24hr")) {
	    start = end - (24 * 60 * 60 * 1000);
	} else if (period.equals("last7d")) {
	    start = end - (7 * 24 * 60 * 60 * 1000);
	} else if (period.equals("last30d")) {
	    start = end - (30L * 24 * 60 * 60 * 1000);
  } else if (period.equals("lastyear")) {
    start = end - (365L * 24 * 60 * 60 * 1000);
	} else if (period.startsWith("custom;")) {
	    
	    // default value is between 2 days ago and now
	    String startString="2 days ago";
	    String endString="now";
	    
	    // tokenize the value to "custom;2 days ago;now" 
	    StringTokenizer st=new StringTokenizer(period,";");
	    if (st.hasMoreTokens()) {
		st.nextToken(); // skip the first token
		if (st.hasMoreTokens()) {
		    startString=st.nextToken();
		    if (st.hasMoreTokens()) {
			endString=st.nextToken();
		    }
		}
	    }
	    
	    // parse the parameter strings
	    start = parseDateShorthand(startString);
	    end = parseDateShorthand(endString);
	}
    }

  public void init(HttpServletRequest request) {
    xf = new XssFilter(request);
    Calendar now = Calendar.getInstance();
    this.session = request.getSession();
    if (request.getParameter("time_type") == null
        && session.getAttribute("time_type") == null
        && session.getAttribute("period") == null
        && request.getParameter("period") == null) {
      end = now.getTimeInMillis();
      start = end - 60 * 60 * 1000;
      session.setAttribute("period", "last1hr");
      session.setAttribute("time_type", "last");
      session.setAttribute("start", "" + start);
      session.setAttribute("end", "" + end);
    } else if (request.getParameter("period") != null
        && !"".equals(request.getParameter("period"))) {
      String period = xf.getParameter("period");
      parsePeriodValue(period);
    } else if (request.getParameter("start") != null
        && request.getParameter("end") != null) {
      start = Long.parseLong(request.getParameter("start"));
      end = Long.parseLong(request.getParameter("end"));
    } else if ("range".equals(session.getAttribute("time_type"))) {
      start = Long.parseLong((String) session.getAttribute("start"));
      end = Long.parseLong((String) session.getAttribute("end"));
    } else if ("last".equals(session.getAttribute("time_type"))
        && session.getAttribute("period") != null) {
      String period = (String) session.getAttribute("period");
      parsePeriodValue(period);
    }

    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd");
    SimpleDateFormat formatHour = new SimpleDateFormat("HH");
    SimpleDateFormat formatMin = new SimpleDateFormat("mm");

    formatter.setTimeZone(this.tz);
    formatDate.setTimeZone(this.tz);
    formatHour.setTimeZone(this.tz);
    formatMin.setTimeZone(this.tz);

    startS = formatter.format(start);
    this.startDate = formatDate.format(start);
    this.startHour = formatHour.format(start);
    this.startMin = formatMin.format(start);
    endS = formatter.format(end);
    this.endDate = formatDate.format(end);
    this.endHour = formatHour.format(end);
    this.endMin = formatMin.format(end);
  }

  public String getStartDate(String format) {
    SimpleDateFormat formatter = new SimpleDateFormat(format);
    formatter.setTimeZone(this.tz);
    return formatter.format(this.start);
  }

  public String getStartDate() {
    return this.startDate;
  }

  public String getStartHour() {
    return this.startHour;
  }

  public String getStartMinute() {
    return this.startMin;
  }

  public String getStartTimeText() {
    return this.startS;
  }

  public long getStartTime() {
    return start;
  }

  public String getEndDate(String format) {
    SimpleDateFormat formatter = new SimpleDateFormat(format);
    formatter.setTimeZone(this.tz);
    return formatter.format(this.end);
  }

  public String getEndDate() {
    return this.endDate;
  }

  public String getEndHour() {
    return this.endHour;
  }

  public String getEndMinute() {
    return this.endMin;
  }

  public String getEndTimeText() {
    return this.endS;
  }

  public long getEndTime() {
    return end;
  }

}
