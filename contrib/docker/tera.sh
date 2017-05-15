export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/apache/hadoop/bin:/opt/apache/hbase/bin
su hdfs -c "hadoop dfs -mkdir -p /user/hdfs"
while :
do
  su hdfs -c "hadoop jar /opt/apache/hadoop-2.7.2/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.2.jar teragen 100 /user/hdfs/terasort-input"
  su hdfs -c "hadoop jar /opt/apache/hadoop-2.7.2/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.2.jar terasort /user/hdfs/terasort-input /user/hdfs/terasort-output"
  su hdfs -c "hadoop dfs -rmr  -skipTrash /user/hdfs/terasort-input/"
  su hdfs -c "hadoop dfs -rmr  -skipTrash /user/hdfs/terasort-output/"
done
