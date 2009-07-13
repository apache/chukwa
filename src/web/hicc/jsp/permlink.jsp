<%
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
%>
<%@ page import = "javax.servlet.http.*, java.net.URLEncoder, java.sql.*,java.io.*, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, java.util.*" %>
<%!
   public static String HTMLEntityEncode( String s ) {
       StringBuffer buf = new StringBuffer();
       int len = (s == null ? -1 : s.length());
       for ( int i = 0; i < len; i++ ) {
           char c = s.charAt( i );
           if ( c>='a' && c<='z' || c>='A' && c<='Z' || c>='0' && c<='9' ) {
               buf.append( c );
           } else {
               buf.append( "&#" + (int)c + ";" );
           }
       }
       return buf.toString();
   }
%>
<%
   StringBuffer buf = new StringBuffer();
   for (Enumeration e = session.getAttributeNames() ; e.hasMoreElements() ;) {
       String name = (String) e.nextElement();
       if(name.indexOf("_session.cache.")==-1) {
           buf.append("_session."+name+"="+URLEncoder.encode(session.getAttribute(name).toString(),"UTF-8"));
       }
       buf.append("&");
   }
   out.println(buf.toString());
%>
