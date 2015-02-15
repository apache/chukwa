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

import org.apache.log4j.Logger;

public class NagiosHelper {
  static Logger log = Logger.getLogger(NagiosHelper.class);
  public static final int NAGIOS_OK       = 0;
  public static final int NAGIOS_WARN     = 1;
  public static final int NAGIOS_CRITICAL = 2; 
  public static final int NAGIOS_UNKNOWN  = 3;
  
  public static void sendNsca(String msg, int state) {
    if(state==NAGIOS_OK) {
      log.info(msg);
    } else if (state==NAGIOS_WARN) {
      log.warn(msg);
    } else if (state==NAGIOS_CRITICAL) {
      log.error(msg);
    } else if (state==NAGIOS_UNKNOWN) {
      log.warn(msg);
    }
  }
}
