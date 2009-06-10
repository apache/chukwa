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

package org.apache.hadoop.chukwa.dataloader;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

public abstract class DataLoaderFactory {

  static ChukwaConfiguration conf = null;
  static FileSystem fs = null;
  protected FileStatus[] source = null;

  private static Log log = LogFactory.getLog(DataLoaderFactory.class);

  public DataLoaderFactory() {
  }

  /**
   * @param args
   * @throws IOException
   */
  public void load(ChukwaConfiguration conf, FileSystem fs, FileStatus[] src) throws IOException {
    this.source=src;
    this.conf=conf;
    this.fs=fs;
  }

}
