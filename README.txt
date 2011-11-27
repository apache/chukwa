Chukwa 0.5 -- April 2010

This is the second formal release of Chukwa, an Apache Hadoop subproject 
dedicated to scalable log collection and processing. If you have large 
volumes of log data generated across a cluster, and you need to process 
them with MapReduce, Chukwa may be the tool for you.

The notes for this release are in docs/releasenotes.html

BUILDING CHUKWA

To build chukwa from source:

mvn clean package

To check that things are ok, run 'ant test'. It should take roughly fifteen minutes.

RUNNING CHUKWA

If you are unfamiliar with Chukwa, you should start by reading the design 
overview, in docs/design.html. This will tell you what each piece of Chukwa
does.  

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

Full installation instructions are in docs/admin.html. 

