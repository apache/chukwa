package org.apache.hadoop.chukwa.extraction.engine;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.Chunk;

/**
 * Various utility methods.
 * 
 */
public class RecordUtil {
  static Pattern clusterPattern = Pattern
      .compile("(.*)?cluster=\"(.*?)\"(.*)?");

  public static String getClusterName(Record record) {
    String tags = record.getValue(Record.tagsField);
    if (tags != null) {
      Matcher matcher = clusterPattern.matcher(tags);
      if (matcher.matches()) {
        return matcher.group(2);
      }
    }

    return "undefined";
  }
  /**
   * Uses a precompiled pattern, so theoretically faster than
   * Chunk.getTag().
   * 
   */
  public static String getClusterName(Chunk chunk) {
    String tags = chunk.getTags();
    if (tags != null) {
      Matcher matcher = clusterPattern.matcher(tags);
      if (matcher.matches()) {
        return matcher.group(2);
      }
    }

    return "undefined";
  }
  

}
