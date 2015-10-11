package org.apache.hadoop.chukwa.datacollection.writer.parquet;

import org.apache.avro.Schema;

public class ChukwaAvroSchema {
  public static Schema getSchema() {
    String input = "{\"namespace\": \"chukwa.apache.org\"," +
        "\"type\": \"record\"," +
        "\"name\": \"Chunk\"," +
        "\"fields\": [" +
            "{\"name\": \"dataType\", \"type\": \"string\"}," +
            "{\"name\": \"data\", \"type\": \"bytes\"}," +
            "{\"name\": \"source\", \"type\": \"string\"}," +
            "{\"name\": \"stream\", \"type\": \"string\"}," +
            "{\"name\": \"tags\", \"type\": \"string\"}," +
            "{\"name\": \"seqId\",  \"type\": [\"long\", \"null\"]}" +
        "]"+
       "}";

      // load your Avro schema
    Schema avroSchema = new Schema.Parser().parse(input);
    return avroSchema;
  }
}
