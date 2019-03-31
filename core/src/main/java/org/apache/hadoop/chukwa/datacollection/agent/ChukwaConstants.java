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

package org.apache.hadoop.chukwa.datacollection.agent;

public class ChukwaConstants {
  public static final String SSL_ENABLE = "chukwa.ssl.enable";
  public static final String KEYSTORE_STORE = "chukwa.ssl.keystore.store";
  public static final String KEYSTORE_PASSWORD = "chukwa.ssl.keystore.password";
  public static final String KEYSTORE_KEY_PASSWORD = "chukwa.ssl.keystore.key.password";
  public static final String KEYSTORE_TYPE = "chukwa.ssl.keystore.type";
  public static final String TRUSTSTORE_STORE = "chukwa.ssl.truststore.store";
  public static final String TRUST_PASSWORD = "chukwa.ssl.trust.password";
  public static final String TRUSTSTORE_TYPE = "chukwa.ssl.truststore.type";
  public static final String SSL_PROTOCOL = "chukwa.ssl.protocol";
  public static final String DEFAULT_SSL_PROTOCOL = "TLS";
  public static final String DEFAULT_STORE_TYPE = "JKS";
}
