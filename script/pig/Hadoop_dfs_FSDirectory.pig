register $chukwaCore
register $chukwaPig
define chukwaLoader org.apache.hadoop.chukwa.ChukwaStorage();
define timePartition_Hadoop_dfs_FSDirectory_$timePeriod org.apache.hadoop.chukwa.TimePartition('$timePeriod');
define seqWriter_Hadoop_dfs_FSDirectory_$timePeriod org.apache.hadoop.chukwa.ChukwaStorage('c_timestamp','c_recordtype', 'c_application', 'c_cluster','c_source' , 'files_deleted');
A_Hadoop_dfs_FSDirectory_$timePeriod = load '$input' using  chukwaLoader as (ts: long,fields);
B_Hadoop_dfs_FSDirectory_$timePeriod = FOREACH A_Hadoop_dfs_FSDirectory_$timePeriod GENERATE timePartition_Hadoop_dfs_FSDirectory_$timePeriod(ts) as time ,fields#'csource' as g0 , fields#'files_deleted' as f0;
C_Hadoop_dfs_FSDirectory_$timePeriod = group B_Hadoop_dfs_FSDirectory_$timePeriod by (time,g0 );
D_Hadoop_dfs_FSDirectory_$timePeriod = FOREACH C_Hadoop_dfs_FSDirectory_$timePeriod generate group.time as ts, '$recType', 'downsampling $timePeriod', '$cluster', group.g0 , AVG(B_Hadoop_dfs_FSDirectory_$timePeriod.f0) as f0;
-- describe D_Hadoop_dfs_FSDirectory_$timePeriod;
-- dump D_Hadoop_dfs_FSDirectory_$timePeriod;
store D_Hadoop_dfs_FSDirectory_$timePeriod into '$output' using seqWriter_Hadoop_dfs_FSDirectory_$timePeriod;
