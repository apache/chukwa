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
package org.apache.hadoop.chukwa.hicc.rest;

import java.lang.reflect.Type;

import javax.servlet.ServletContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

import org.apache.log4j.Logger;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;

import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;

@Provider
public class VelocityResolver implements InjectableProvider<Context, Type> {
  @Context
  private ServletContext servletContext;

  private VelocityEngine ve;
  private static Logger LOG = Logger.getLogger(VelocityResolver.class);
  public static String LOGGER_NAME = VelocityResolver.class.getName();
  
  /**
   * Jersey configuration for setting up Velocity configuration.
   */
  @Override
  public Injectable<VelocityEngine> getInjectable(ComponentContext arg0,
      Context arg1, Type c) {
    if (c.equals(VelocityEngine.class)) {
      return new Injectable<VelocityEngine>() {
        public VelocityEngine getValue() {
          if (ve == null) {
            LOG.info("Ready to start velocity");
            ve = new VelocityEngine();
            ve.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS,
                    "org.apache.velocity.runtime.log.Log4JLogChute");
            ve.setProperty("runtime.log.logsystem.log4j.logger",
                LOGGER_NAME);
            ve.setProperty(RuntimeConstants.RESOURCE_LOADER,
                "webapp");
            ve.setProperty("webapp.resource.loader.class",
                    "org.apache.velocity.tools.view.WebappResourceLoader");
            ve.setProperty("webapp.resource.loader.path",
                "/WEB-INF/vm/");
            ve.setApplicationAttribute(
                "javax.servlet.ServletContext", servletContext);
            try {
              ve.init();
              LOG.info("Velocity is loaded");
            } catch (Exception e) {
              LOG.error("Error when initializing Velocity", e);
            }
          }
          return ve;
        }
      };
    }
    return null;
  }

  public ComponentScope getScope() {
    return ComponentScope.Singleton;
  }

}
