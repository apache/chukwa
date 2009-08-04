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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.extraction.engine.RecordUtil;


public class Filter {
  
  private static final String[] SEARCH_TARGS = 
  {"datatype", "name", "host", "cluster", "content"};
  static final String SEPARATOR="&";
  
  private static class SearchRule {
    Pattern p;
    String targ;
    
    SearchRule(Pattern p, String t) {
      this.p = p;
      this.targ = t;
    }
    
    boolean matches(Chunk chunk) {
      if(targ.equals("datatype")) {
        return p.matcher(chunk.getDataType()).matches();
      } else if(targ.equals("name")) {
        return p.matcher(chunk.getStreamName()).matches();
      } else if(targ.equals("host")) {
        return p.matcher(chunk.getSource()).matches();
      } else if(targ.equals("cluster")) {
        String cluster = RecordUtil.getClusterName(chunk);
        return p.matcher(cluster).matches();
      } else if(targ.equals("content")) {
        String content = new String(chunk.getData());
        return p.matcher(content).matches();
      } else if(targ.startsWith("tags.")) {
        String tagName = targ.substring("tags.".length());
        String tagVal = chunk.getTag(tagName);
        if(tagVal == null)
          return false;
        return p.matcher(tagVal).matches();
      } else { 
        assert false: "unknown target: " +targ;
        return false;
      }
    }
    
    public String toString() {
      return targ + "=" +p.toString();
    } 
  }

  List<SearchRule> compiledPatterns;
    
  public Filter(String listOfPatterns) throws  PatternSyntaxException{
    compiledPatterns = new ArrayList<SearchRule>();
    //FIXME: could escape these
    String[] patterns = listOfPatterns.split(SEPARATOR);
    for(String p: patterns) {
      int equalsPos = p.indexOf('=');
      
      if(equalsPos < 0 || equalsPos > (p.length() -2)) {
        throw new PatternSyntaxException(
            "pattern must be of form targ=pattern", p, -1);
      }
      
      String targ = p.substring(0, equalsPos);
      if(!targ.startsWith("tags.") && !ArrayUtils.contains(SEARCH_TARGS, targ)) {
        throw new PatternSyntaxException(
            "pattern doesn't start with recognized search target", p, -1);
      }
      
      Pattern pat = Pattern.compile(p.substring(equalsPos+1), Pattern.DOTALL);
      compiledPatterns.add(new SearchRule(pat, targ));
    }
  }

  public boolean matches(Chunk chunk) {
    for(SearchRule r: compiledPatterns) {
      if(!r.matches(chunk))
        return false;
    }
    return true;
  }
  
  public int size() {
    return compiledPatterns.size();
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(compiledPatterns.get(0));
    for(int i=1; i < compiledPatterns.size(); ++i) {
      sb.append(" & ");
      sb.append(compiledPatterns.get(i));
    }
    return sb.toString();
  }
}//end class
