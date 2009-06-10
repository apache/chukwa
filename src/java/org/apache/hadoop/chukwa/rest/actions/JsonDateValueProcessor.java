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

import java.util.Date;
import java.text.*;
import java.sql.Timestamp;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;
import net.sf.json.processors.JsonValueProcessor;
import org.apache.commons.logging.*;

public class JsonDateValueProcessor implements JsonValueProcessor {
    private static String OUTPUT_FORMAT="yyyy-MM-dd HH:mm:ss";
    protected static Log log = LogFactory.getLog(JsonDateValueProcessor.class);

    public Object processArrayValue(Object value, JsonConfig jsonConfig) {
	return process(value);
    }
    
    public Object processObjectValue(String key, Object value,
				     JsonConfig jsonConfig) {
	return process(value);
    }
    
    private Object process(Object value) {
	DateFormat dateFormat = new SimpleDateFormat(OUTPUT_FORMAT);
	if (value == null) {
	    return "";
	}
	if(value instanceof Timestamp)
	    return dateFormat.format((Timestamp) value);
	else 
	    return dateFormat.format((Date) value);
    }
}


