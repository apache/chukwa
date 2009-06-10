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

package org.apache.hadoop.chukwa.rest.actions;

import java.util.*;
import org.apache.commons.logging.*;
import org.apache.commons.lang.time.*;
import java.lang.reflect.*;
import net.sf.json.*;
import net.sf.json.processors.JsonValueProcessor;

public class RestController  {
    protected static Log log = LogFactory.getLog(RestController.class);

    private static String convertObjectToXml(Object obj) {
	StringBuilder s=new StringBuilder();
	s.append("<item>");
	try {
            Class cls = obj.getClass();
        
            Field fieldlist[] 
		= cls.getDeclaredFields();
            for (int i = 0; i < fieldlist.length; i++) {
		Field fld = fieldlist[i];
		String fldName = fld.getName();
		String functionName = "get"+ fldName.substring(0,1).toUpperCase() + fldName.substring(1);
		String value = "";
                @SuppressWarnings("unchecked")
		Method meth=cls.getMethod(functionName);
		Object oret = meth.invoke(obj);
		if (oret == null) {
		    value="";
		} else if ((oret instanceof Date) || (oret instanceof java.sql.Timestamp)) {
		    
		    java.sql.Timestamp d = (java.sql.Timestamp) oret;
		    
		    long time = d.getTime();
		    String date = DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss");
		    value = date;
		} else {
		    value = oret.toString();
		}

		s.append("<"+fldName+">"+value+"</"+fldName+">");
            }
	    s.append("\n");
	}
	catch (Throwable e) {
	    System.err.println(e);
	}
	s.append("</item>");
	return s.toString();
    }

    private static String getObjectFields(Object obj) {
	StringBuilder s=new StringBuilder();
	try {
            Class cls = obj.getClass();

            Method methlist[] 
		= cls.getDeclaredMethods();
	    int count=0;
            for (int i = 0; i < methlist.length;
		 i++) {  
		Method m = methlist[i];
		if (m.getName().startsWith("get")) {
		    String name=m.getName().substring(3);
		    if (count!=0) {
			s.append(",");
		    }	
		    count+=1;
		    s.append("\""+name+"\"");
		}
            }
	    s.append("\n");
	}
	catch (Throwable e) {
	    System.err.println(e);
	}
	return s.toString();

    }

    private static String getObjectValues(Object obj) {
	StringBuilder s=new StringBuilder();
        try {
	    Class cls = obj.getClass();
        
            Method methlist[] 
		= cls.getDeclaredMethods();
	    int count=0;
            for (int i = 0; i < methlist.length;
		 i++) {  
		Method m = methlist[i];
		if (m.getName().startsWith("get")) {
		    String name=m.getName();
                    @SuppressWarnings("unchecked")
		    Method meth=cls.getMethod(name);
		    Object oret = meth.invoke(obj);
		    if (count!=0) {
			s.append(",");
		    }
		    count+=1;
		    if (oret == null) {
			s.append("\"\"");
		    } else if ((oret instanceof Date) || (oret instanceof java.sql.Timestamp)) {
			long time=0;
			if (oret instanceof Date) {
			    Date d = (Date) oret;
			    time = d.getTime();
			} else if (oret instanceof java.sql.Timestamp) {
			    java.sql.Timestamp d = (java.sql.Timestamp) oret;
			    time = d.getTime();
			}

                        String date = DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss");
			s.append("\""+date+"\"");
		    } else {
			s.append("\""+oret.toString()+"\"");
		    }
		}
	    }
	    s.append("\n");
	}
	catch (Throwable e) {
            System.err.println(e);
	}
	return s.toString();
    }

    
    protected static String convertToJson(Object obj) {
	String str="";

        JsonConfig config = new JsonConfig();
	config.registerJsonValueProcessor(Date.class,new JsonDateValueProcessor());
	config.registerJsonValueProcessor(java.sql.Timestamp.class,new JsonDateValueProcessor()); 

        if (obj != null) {
            if (isArray(obj)) {

                JSONArray jsonArray = JSONArray.fromObject(obj, config);
                str=jsonArray.toString();
            } else {

                JSONObject jsonObject = JSONObject.fromObject(obj, config);
                str=jsonObject.toString();
            }
        }

	return str;
    }

    protected static String convertToXml(Object obj) {
	StringBuilder s=new StringBuilder();
	s.append("<items>");
	if ( obj != null) {
	    if (isArray(obj)) {
		Iterator iterator = ((Collection)obj).iterator();
		while (iterator.hasNext()) {
		    Object element = iterator.next();
		    s.append(convertObjectToXml(element));
		}
	    } else {
		s.append(convertObjectToXml(obj));
	    }
	}
	s.append("</items>");
	return s.toString();
    }

    protected static String convertToCsv(Object obj) {
	StringBuilder str=new StringBuilder();
	if ( obj != null) {
	    if (isArray(obj)) {
		boolean first=true;
		Iterator iterator = ((Collection)obj).iterator();
		while (iterator.hasNext()) {
		    Object element = iterator.next();
		    if (first) {
			first=false;
			str.append(getObjectFields(element));
		    }
		    str.append(getObjectValues(element));
		}
	    } else {
		str.append(getObjectFields(obj));
		str.append(getObjectValues(obj));
	    }
	}
	return str.toString();
    }

    private static boolean isArray(Object obj) {
        return obj instanceof Collection || obj.getClass().isArray();
    }


    
}
