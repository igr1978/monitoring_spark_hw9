#!/bin/bash

echo "STARTING APP HW"

# Run Spark example
#${SPARK_HOME}/bin/spark-submit --class org.apache.spark.examples.JavaSparkPi ${SPARK_HOME}/examples/jars/spark-examples_2.12-3.0.1.jar


${SPARK_HOME}/bin/spark-submit  --class HomeWorkTaxiRDD hadoop_hw3_9-assembly-0.1.jar
${SPARK_HOME}/bin/spark-submit  --class HomeWorkTaxiDF hadoop_hw3_9-assembly-0.1.jar
${SPARK_HOME}/bin/spark-submit  --class HomeWorkTaxiDS hadoop_hw3_9-assembly-0.1.jar


${SPARK_HOME}/bin/spark-submit --class org.apache.spark.examples.JavaSparkPi ${SPARK_HOME}/examples/jars/spark-examples_2.12-3.0.1.jar