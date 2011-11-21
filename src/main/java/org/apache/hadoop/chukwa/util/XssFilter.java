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

package org.apache.hadoop.chukwa.util;

import java.util.Enumeration;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.josephoconnell.html.HTMLInputFilter;

public class XssFilter {
    private HttpServletRequest request = null;
    private static Log log = LogFactory.getLog(XssFilter.class);
    private HttpSession session = null;

    public XssFilter() {
    }

    public XssFilter(HttpServletRequest request) {
      this.request = request;
      try {
        this.session = request.getSession();
        for (Enumeration e = request.getParameterNames() ; e.hasMoreElements() ;) {
          Pattern p = Pattern.compile("_session\\.(.*)");
          String name = (String) e.nextElement();
          Matcher matcher = p.matcher(name);
          if(matcher.find()) {
            String realName = matcher.group(1);
            if(session!=null) {
              session.setAttribute(realName,filter(request.getParameter(name)));
            }
          }
        }
      } catch(NullPointerException ex) {
        // Do nothing if session does not exist.
      }
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
