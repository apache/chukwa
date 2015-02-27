#Apache Chukwa Project

<img src="http://chukwa.apache.org/images/chukwa_logo_small.jpg" align="right" width="300" />

Chukwa is an open source data collection system for monitoring large distributed systems. 
Chukwa is built on top of the Hadoop Distributed File System (HDFS) and Map/Reduce 
framework and inherits Hadoop’s scalability and robustness. Chukwa also includes a 
ﬂexible and powerful toolkit for displaying, monitoring and analyzing results to 
make the best use of the collected data. 

##Overview

Log processing was one of the original purposes of MapReduce. Unfortunately, using 
Hadoop MapReduce to monitor Hadoop can be inefficient. Batch processing nature of 
Hadoop MapReduce prevents the system to provide real time status of the cluster.

We started this journey at beginning of 2008, and a lot of Hadoop components have 
been built to improve overall reliability of the system and improve realtimeness of 
monitoring. We have adopted HBase to facilitate lower latency of random reads and 
using in memory updates and write ahead logs to improve the reliability for root 
cause analysis.

Logs are generated incrementally across many machines, but Hadoop MapReduce works 
best on a small number of large files. Merging the reduced output of multiple runs 
may require additional mapreduce jobs. This creates some overhead for data management 
on Hadoop.

Chukwa is a Hadoop subproject devoted to bridging that gap between logs processing 
and Hadoop ecosystem. Chukwa is a scalable distributed monitoring and analysis system, 
particularly logs from Hadoop and other distributed systems.

The Chukwa Documentation provides the information you need to get started using 
Chukwa. <a href="http://chukwa.apache.org/docs/r0.6.0/design.html">Architecture and 
Design document</a> provides high level view of Chukwa design.

If you're trying to set up a Chukwa cluster from scratch, 
<a href="http://chukwa.apache.org/docs/r0.6.0/user.html">User Guide</a> describes the 
setup and deploy procedure.

If you want to configure the Chukwa agent process, to control what's collected, you 
should read the <a href="http://chukwa.apache.org/docs/r0.6.0/agent.html">Agent Guide</a>. 
There is also a <a href="http://chukwa.apache.org/docs/r0.6.0/pipeline.html">Pipeline Guide</a> 
describing configuration parameters for ETL processes for the data pipeline.

And if you want to develop Chukwa to monitor other data sources, 
<a href="http://chukwa.apache.org/docs/r0.6.0/programming.html"Programming Guide</a> 
maybe handy to learn about Chukwa programming API.

If you have more questions, you can ask on the 
<a href="http://chukwa.apache.org/mail-lists.html">Chukwa mailing lists</a>

##Bulding Chukwa

To build Chukwa from source you require <a href="http://maven.apache.org">Apache Maven</a>:
```
mvn clean package
```
To check that things are ok, run 
```ant test
```
tests should take and run successfully after roughly fifteen minutes.

##Running Cukwa

Users should definately begin with the 
<a href="http://chukwa.apache.org/docs/r0.6.0/Quick_Start_Guide.html">
Chukwa Quick Start Guide</a>

If you're impatient, the following is the 30-second explanation:

The minimum you need to run Chukwa are agents on each machine you're 
monitoring, and a collector to write the collected data to HDFS.  The
basic command to start an agent is bin/chukwa agent.  The base command to
start a collector is bin/chukwa collector.

If you want to start a bunch of agents, you can use the
bin/start-agents.sh script. This just uses ssh to start agents on a
list of machines, given in conf/agents. It's exactly parallel to
Hadoop's start-hdfs and start-mapred scripts.  There's also a 
bin/start-collectors.sh that does the same to start collectors, on 
machines listed in conf/collectors.  One hostname per line.

There are stop scripts that do the exact opposite of the start commands. 

