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
package org.apache.hadoop.chukwa.rest.resource;

import com.sun.jersey.api.json.JSONJAXBContext;
import org.apache.hadoop.chukwa.rest.bean.ViewBean;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;

  @Provider
  public class ViewContextResolver implements ContextResolver<JAXBContext> {
      private JAXBContext context;
      private Set<Class<?>> types;
      protected Class<?>[] classTypes = new Class[] {ViewBean.class};
      protected Set<String> jsonArray = new HashSet<String>(5) {
        {
            add("pages");
            add("layout");
            add("colSize");
            add("widgets");
            add("parameters");
            add("options");
        }
      };

      public ViewContextResolver() throws Exception {
          Map props = new HashMap<String, Object>();
          props.put(JSONJAXBContext.JSON_NOTATION, JSONJAXBContext.JSONNotation.MAPPED);
          props.put(JSONJAXBContext.JSON_ROOT_UNWRAPPING, Boolean.TRUE);
          props.put(JSONJAXBContext.JSON_ARRAYS, jsonArray);
          this.types = new HashSet<Class<?>>(Arrays.asList(classTypes));
          this.context = new JSONJAXBContext(classTypes, props);
      }

      public JAXBContext getContext(Class<?> objectType) {
          return (types.contains(objectType)) ? context : null;
      }

//    private final JAXBContext context;
//
//    public ViewContextResolver() throws Exception {
//      this.context = new JSONJAXBContext(JSONConfiguration.natural().build(), "package.of.your.model");
//  }
//
//  public JAXBContext getContext(Class<?> objectType) {
//      return context;
//  }

  }

