export DD_HOME=/home/lieu/dev/spark
export SPARK_HOME=$DD_HOME/spark-2.4.4-bin-without-hadoop
export PATH=$PATH:$SPARK_HOME/bin
export HADOOP_HOME=$DD_HOME/hadoop-3.1.2
export PATH=$PATH:$HADOOP_HOME/bin
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export TERM=xterm-color

sbt package
$SPARK_HOME/bin/spark-submit \
  --class "SimpleApp" \
  --master spark://192.168.10.66:7077 \
  --jars $DD_HOME/bin/slf4j-api-1.7.25.jar,$DD_HOME/bin/slf4j-log4j12-1.7.25.jar,$DD_HOME/bin/aws-java-sdk-1.11.624.jar,$DD_HOME/bin/aws-java-sdk-core-1.11.624.jar,$DD_HOME/bin/aws-java-sdk-dynamodb-1.11.624.jar,$DD_HOME/bin/aws-java-sdk-kms-1.11.624.jar,$DD_HOME/bin/aws-java-sdk-s3-1.11.624.jar,$DD_HOME/bin/hadoop-aws-3.1.2.jar,$DD_HOME/bin/httpclient-4.5.9.jar,$DD_HOME/bin/joda-time-2.10.3.jar \
  target/scala-2.11/simple-project_2.11-1.0.jar
