package org.apache.hadoop.chukwa.rest.resource;

import com.sun.jersey.api.json.JSONJAXBContext;

import org.apache.hadoop.chukwa.rest.bean.CatalogBean;
import org.apache.hadoop.chukwa.rest.bean.CategoryBean;
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
  public class WidgetContextResolver implements ContextResolver<JAXBContext> {
      private JAXBContext context;
      private Set<Class<?>> types;
      protected Class<?>[] classTypes = new Class[] {CatalogBean.class, CategoryBean.class};
      protected Set<String> jsonArray = new HashSet<String>(1) {
        {
            add("children");
        }
      };

      public WidgetContextResolver() throws Exception {
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
  }

