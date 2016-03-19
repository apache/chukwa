# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

Chukwa SolrCloud Integration

Usage
-----

- Extract Solr tarball and create solr collections

  tar fxv solr-5.5.0.tar.gz
  bin/solr start -cloud -z [zookeeper_host]:2181
  bin/solr create_collection -c chukwa -n chukwa

- Configure chukwa-agent-conf.xml with pipeline that includes SolrWriter.

  <property>
    <name>chukwa.pipeline</name>
    <value>org.apache.hadoop.chukwa.datacollection.writer.solr.SolrWriter</value>
    <description>Configure agent to write to solr</description>
  </property>

  <property>
    <name>solr.cloud.address</name>
    <value>localhost:2181</value>
    <description>Solr cloud zookeeper address</description>
  </property>

  <property>
    <name>solr.collection</name>
    <value>chukwa</value>
    <description>Solr Cloud collection name</description>
  </property>

- Restart Chukwa Agent and point browser to:

  http://localhost:8983/solr/logs/select?q=*:*&wt=json&indent=true

This REST API will display all collected log entries.
