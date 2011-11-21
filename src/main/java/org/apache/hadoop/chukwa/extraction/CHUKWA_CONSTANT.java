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

package org.apache.hadoop.chukwa.extraction;

public interface CHUKWA_CONSTANT {
  public static final String HDFS_DEFAULT_NAME_FIELD = "fs.default.name";
  public static final String WRITER_HDFS_FILESYSTEM_FIELD = "writer.hdfs.filesystem";
  public static final String CHUKWA_ROOT_DIR_FIELD = "chukwaRootDir";
  public static final String CHUKWA_ROOT_REPOS_DIR_FIELD = "chukwaRootReposDir";

  
  //This is the INPUT directory for archiving; defaults to /chukwa/logs
  public static final String CHUKWA_ARCHIVE_DIR_FIELD = "chukwaArchiveDir";
  public static final String CHUKWA_POST_PROCESS_DIR_FIELD = "chukwaPostProcessDir";
  public static final String CHUKWA_POSTPROCESS_IN_ERROR_DIR_FIELD = "chukwaPostProcessInErrorDir";
  public static final String CHUKWA_DATA_SINK_DIR_FIELD = "chukwaDataSinkDir";

  public static final String CHUKWA_NAGIOS_HOST_FIELD = "demux.nagiosHost";
  public static final String CHUKWA_NAGIOS_PORT_FIELD = "demux.nagiosPort";
  public static final String CHUKWA_REPORTING_HOST_FIELD = "demux.reportingHost4Nagios";
  
  
  public static final String CHUKWA_POSTPROCESS_MAX_ERROR_COUNT_FIELD = "post.process.max.error.count.before.shutdown";
  public static final String CHUKWA_ARCHIVE_MAX_ERROR_COUNT_FIELD     = "archive.max.error.count.before.shutdown";
  public static final String CHUKWA_DEMUX_MAX_ERROR_COUNT_FIELD       = "demux.max.error.count.before.shutdown";

  public static final String CHUKWA_DEMUX_REDUCER_COUNT_FIELD     = "demux.reducerCount";

  public static final String DEFAULT_CHUKWA_ROOT_DIR_NAME          = "/chukwa/";
  public static final String DEFAULT_REPOS_DIR_NAME               = "repos/";
  public static final String DEFAULT_CHUKWA_POSTPROCESS_DIR_NAME  = "postProcess/";
  public static final String DEFAULT_POSTPROCESS_IN_ERROR_DIR_NAME = "postProcessInError/";
  public static final String DEFAULT_CHUKWA_LOGS_DIR_NAME         = "logs/";
  
  public static final String DEFAULT_DEMUX_PROCESSING_DIR_NAME    = "demuxProcessing/";
  public static final String DEFAULT_DEMUX_MR_OUTPUT_DIR_NAME     = "mrOutput/";
  public static final String DEFAULT_DEMUX_MR_INPUT_DIR_NAME      = "mrInput/";
  public static final String DEFAULT_DEMUX_IN_ERROR_DIR_NAME      = "inError/";
  
  public static final String DEFAULT_CHUKWA_DATASINK_DIR_NAME     = "dataSinkArchives/";
  public static final String DEFAULT_FINAL_ARCHIVES               = "finalArchives/";
  
    //These fields control the working dirs for the archive mapred job.
    //They are not configurable at runtime.
  public static final String ARCHIVES_PROCESSING_DIR_NAME    = "archivesProcessing/";
  public static final String ARCHIVES_MR_OUTPUT_DIR_NAME     = "mrOutput/";
  public static final String ARCHIVES_MR_INPUT_DIR_NAME      = "mrInput/";
  public static final String ARCHIVES_IN_ERROR_DIR_NAME      = "inError/";

  public static final String POST_DEMUX_DATA_LOADER = "chukwa.post.demux.data.loader";  
  public static final String POST_DEMUX_SUCCESS_ACTION = "chukwa.post.demux.success.action";  
}
