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
