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

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class AdaptorNamingUtils {
  
  public static String synthesizeAdaptorID(String adaptorClassName, String dataType,
      String params) throws NoSuchAlgorithmException {
    MessageDigest md;
   md = MessageDigest.getInstance("MD5");

    md.update(adaptorClassName.getBytes(Charset.forName("UTF-8")));
    md.update(dataType.getBytes(Charset.forName("UTF-8")));
    md.update(params.getBytes(Charset.forName("UTF-8")));
    StringBuilder sb = new StringBuilder();
    sb.append("adaptor_");
    byte[] bytes = md.digest();
    for(int i=0; i < bytes.length; ++i) {
      if( (bytes[i] & 0xF0) == 0)
        sb.append('0');
      sb.append( Integer.toHexString(0xFF & bytes[i]) );
    }
    return sb.toString();
   
  }

}
