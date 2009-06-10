register $chukwaCore
register $chukwaPig
define chukwaLoader org.apache.hadoop.chukwa.ChukwaStorage();
define timePartition_Df_$timePeriod org.apache.hadoop.chukwa.TimePartition('$timePeriod');
define seqWriter_Df_$timePeriod org.apache.hadoop.chukwa.ChukwaStorage('c_timestamp','c_recordtype', 'c_application', 'c_cluster','c_source' ,'Mounted on' , 'Available', 'Use%', 'Used');
A_Df_$timePeriod = load '$input' using  chukwaLoader as (ts: long,fields);
B_Df_$timePeriod = FOREACH A_Df_$timePeriod GENERATE timePartition_Df_$timePeriod(ts) as time ,fields#'csource' as g0 ,fields#'Mounted on' as g1 , fields#'Available' as f0, fields#'Use%' as f1, fields#'Used' as f2;
C_Df_$timePeriod = group B_Df_$timePeriod by (time,g0 ,g1 );
D_Df_$timePeriod = FOREACH C_Df_$timePeriod generate group.time as ts, '$recType', 'downsampling $timePeriod', '$cluster', group.g0 , group.g1 , AVG(B_Df_$timePeriod.f0) as f0, AVG(B_Df_$timePeriod.f1) as f1, AVG(B_Df_$timePeriod.f2) as f2;
-- describe D_Df_$timePeriod;
-- dump D_Df_$timePeriod;
store D_Df_$timePeriod into '$output' using seqWriter_Df_$timePeriod;
