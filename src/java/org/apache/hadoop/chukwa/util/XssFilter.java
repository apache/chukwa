package org.apache.hadoop.chukwa.util;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.josephoconnell.html.HTMLInputFilter;

public class XssFilter {
    private HttpServletRequest request = null;
    private static Log log = LogFactory.getLog(XssFilter.class);
    
    public XssFilter() {
    }

    public XssFilter(HttpServletRequest request) {
      this.request = request;
    }
    
    public String getParameter(String key) {
	String value=null;
	try {
	    value=this.request.getParameter(key);  
	} catch (Exception e) {
	    log.info("XssFilter.getParameter: Cannot get parameter for: "+key);
	}
	return filter(value);
    }
    
    public String[] getParameterValues(String key) {
      String[] values=null;
      try {
	  values  = this.request.getParameterValues(key);
	  if(values!=null) {
	      for(int i=0;i<values.length;i++) {
		  values[i] = filter(values[i]);
	      }
	  }
      } catch (Exception e) {
	  log.info("XssFilter.getParameterValues: cannot get parameter for: "+key);
      }
      return values;
    }
    
    public String filter( String input ) {
        if(input==null) {
            return null;
        }
        String clean = new HTMLInputFilter().filter( input.replaceAll("\"", "%22").replaceAll("\'","%27"));
        return clean.replaceAll("<", "%3C").replaceAll(">", "%3E");
    }
}
