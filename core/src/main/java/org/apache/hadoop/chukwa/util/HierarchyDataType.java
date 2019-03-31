/*
 * Copyright The Apache Software Foundation
 *
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.chukwa.extraction.CHUKWA_CONSTANT;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

/**
 * To support hierarchyDataType according to CHUKWA-648, which is quite similar
 * to the idea of Hive's Partition. For example, the user can define the
 * dataType as "datatypeLevel1/dataTypeLevel2/dataTypeLevel3" instead of a flat
 * structure like: "datatypeLevel1_datatTypeLeve2_dataTypeLevel3" <BR>
 * <BR>
 * The hierarchyDataType makes the filtering work much more easy when doing the
 * analysis job. For example, if the user focuses on all data under
 * "datatypeLevel1/dataTypeLevel2" category, he only needs to go through all
 * level2 related sub-directories.
 */
public class HierarchyDataType {
  static Logger log = Logger.getLogger(HierarchyDataType.class);

  /**
   * List all matched files under the directory and its sub-dirs
   * @param fs The file system
   * @param path The parent folder
   * @param filter The pattern matcher to filter the required files
   * @param recursive is a flag to search recursively
   * @return list of FileStatus
   */
  public static List<FileStatus> globStatus(FileSystem fs, Path path,
      PathFilter filter, boolean recursive) {
    List<FileStatus> results = new ArrayList<FileStatus>();
    try {
      FileStatus[] candidates = fs.globStatus(path);
      for (FileStatus candidate : candidates) {
        log.debug("candidate is:" + candidate);
        Path p = candidate.getPath();
        if (candidate.isDir() && recursive) {
          StringBuilder subpath = new StringBuilder(p.toString());
          subpath.append("/*");
          log.debug("subfolder is:" + p);
          results.addAll(globStatus(fs, new Path(subpath.toString()), filter,
              recursive));
        } else {
          log.debug("Eventfile is:" + p);
          FileStatus[] qualifiedfiles = fs.globStatus(p, filter);
          if (qualifiedfiles != null && qualifiedfiles.length > 0) {
            log.debug("qualified Eventfile is:" + p);
            Collections.addAll(results, qualifiedfiles);
          }
        }
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    log.debug("results.length: " + results.size());
    return results;
  }

  /**
   * List all files under certain path and its sub-directories
   * @param fs The file system
   * @param path  The parent folder
   * @param recursive is flag to search recursive
   * @return The list of all sub-dirs
   */
  public static List<FileStatus> globStatus(FileSystem fs, Path path,
      boolean recursive) {
    List<FileStatus> results = new ArrayList<FileStatus>();
    try {
      FileStatus[] candidates = fs.listStatus(path);
      if (candidates.length > 0) {
        for (FileStatus candidate : candidates) {
          log.debug("candidate is:" + candidate);
          Path p = candidate.getPath();
          if (candidate.isDir() && recursive) {
            results.addAll(globStatus(fs, p, recursive));
          }
        }
      } else {
        log.debug("path is:" + path);
        results.add(fs.globStatus(path)[0]);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return results;
  }

  /**
   * Get the hierarchyDataType format from the directory. 
   * 
   * @param path The data path
   * @param cluster  The cluster's folder
   * @return The hierarchyDataType
   */
  public static String getDataType(Path path, Path cluster) {
    log.debug("datasource path: " + path + " cluster path: " + cluster);
    String Cluster = cluster.toString();
    if (!Cluster.endsWith("/")) {
      Cluster = Cluster + "/";
    }
    String dataType = path.toString().replaceFirst(Cluster, "");
    log.debug("The datatype is: " + dataType);
    return dataType;
  }

  /**
   * Get the directory without first and last slash mark.
   * 
   * @param datasource is a string
   * @return same string with ending slash trimmed
   */
  public static String trimSlash(String datasource) {
    String results = datasource;
    if (datasource.startsWith("/")) {
      results = datasource.replaceFirst("/", "");
    }
    if (results.endsWith("/")) {
      results = results.substring(0, results.length()-1);
    }
    return results;
  }

  /**
   * Transform the hierarchyDatatType directory into its filename (without any
   * slash mark)
   * 
   * @param datasource is a string
   * @return path to data source
   */
  public static String getHierarchyDataTypeFileName(String datasource){
    return datasource.replace("/", CHUKWA_CONSTANT.HIERARCHY_CONNECTOR);
  }
  
  /**
   * Transform the hierarchyDataType filename into its directory name (with
   * slash mark)
   * 
   * @param datasource is a string
   * @return path to data source
   */
  public static String getHierarchyDataTypeDirectory(String datasource) {
    return datasource.replace(CHUKWA_CONSTANT.HIERARCHY_CONNECTOR, "/");
  }
}
