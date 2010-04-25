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
package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.demux.Demux;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * TsProcessor is a generic processor that can be configured to find the timestamp
 * in the text of a record. By default, this class expects that a record
 * starts with a date in this format: <code>yyyy-MM-dd HH:mm:ss,SSS</code>
 * <P>
 * This format can be changed with the following configurations.
 * <UL>
 * <LI><code>TsProcessor.default.time.format</code> - Changes the default time
 * format used by all data types.</LI>
 * <LI><code>TsProcessor.time.format.[some_data_type]</code> - Overrides the default
 * format for a specific data type.</LI>
 * </UL>
 * If the time string is not at the beginning of the record you can configure a
 * regular expression to locate the timestamp text with either of the following
 * configurations. The text found in group 1 of the regular expression match
 * will be used with the configured date format.
 * <UL>
 * <LI><code>TsProcessor.default.time.regex</code> - Changes the default time
 * location regex of the time text for all data types.</LI>
 * <LI><code>TsProcessor.time.regex.[some_data_type]</code> - Overrides the
 * default time location regex for a specific data type.</LI>
 * </UL>
 *
 */
public class TsProcessor extends AbstractProcessor {
  static Logger log = Logger.getLogger(TsProcessor.class);

  public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss,SSS";

  private Map<String, Pattern> datePatternMap;
  private Map<String, SimpleDateFormat> dateFormatMap;

  public TsProcessor() {
    datePatternMap = new HashMap<String, Pattern>();
    dateFormatMap = new HashMap<String, SimpleDateFormat>();

//    StringBuilder format = new StringBuilder();
//    format.append("TsProcessor.time.format");
//    format.append(chunk.getDataType());
//    if(conf.get(format.toString())!=null) {
//     sdf = new SimpleDateFormat(conf.get(format.toString()));
//    } else {
//     sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
//    }
  }

  @Override
  protected void parse(String recordEntry,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter)
      throws Throwable {
    try {
      SimpleDateFormat sdf = fetchDateFormat(chunk.getDataType());
      Pattern datePattern = fetchDateLocationPattern(chunk.getDataType());
      String dStr = null;

      // fetch the part of the record that contains the date.
      if(datePattern != null) {
        Matcher m = datePattern.matcher(recordEntry);
        if (!m.matches() || m.groupCount() < 1) {
          throw new ParseException("Regex " + datePattern +
                  " couldn't extract date string from record: " + recordEntry, 0);
        }
        else {
          dStr = m.group(1);
        }
      }
      else {
        dStr = recordEntry.substring(0, sdf.toPattern().length());
      }

      Date d = sdf.parse(dStr);
      ChukwaRecord record = new ChukwaRecord();
      this.buildGenericRecord(record, recordEntry, d.getTime(), chunk
          .getDataType());
      output.collect(key, record);
    } catch (ParseException e) {
      log.warn("Unable to parse the date in DefaultProcessor [" + recordEntry
          + "]", e);
      e.printStackTrace();
      throw e;
    } catch (IOException e) {
      log.warn("Unable to collect output in DefaultProcessor [" + recordEntry
          + "]", e);
      e.printStackTrace();
      throw e;
    }

  }
  
  /**
   * For a given dataType, returns the SimpeDateFormat to use.
   * @param dataType
   * @return
   */
  private SimpleDateFormat fetchDateFormat(String dataType) {
    if (dateFormatMap.get(dataType) != null) {
      return dateFormatMap.get(dataType);
    }

    JobConf jobConf = Demux.jobConf;
    String dateFormat = DEFAULT_DATE_FORMAT;

    if (jobConf != null) {
      dateFormat = jobConf.get("TsProcessor.default.time.format", dateFormat);
      dateFormat = jobConf.get("TsProcessor.time.format." + chunk.getDataType(),
                               dateFormat);
    }

    SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
    dateFormatMap.put(dataType, sdf);

    return sdf;
  }

  /**
   * For a given dataType, returns a Pattern that will produce the date portion
   * of the string.
   * @param dataType
   * @return
   */
  private Pattern fetchDateLocationPattern(String dataType) {
    if (datePatternMap.containsKey(dataType)) {
      return datePatternMap.get(dataType);
    }

    JobConf jobConf = Demux.jobConf;
    String datePattern = null;

    if (jobConf != null) {
      datePattern = jobConf.get("TsProcessor.default.time.regex", null);
      datePattern = jobConf.get("TsProcessor.time.regex." + chunk.getDataType(),
                               datePattern);
    }

    Pattern pattern = datePattern != null ? Pattern.compile(datePattern) : null;
    datePatternMap.put(dataType, pattern);

    return pattern;
  }

}
