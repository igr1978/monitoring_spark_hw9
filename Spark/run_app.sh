#!/bin/bash

echo "STARTING APP"

# Run custom python app with enabled metrics for Prometheus
#${SPARK_HOME}/bin/pyspark 
${SPARK_HOME}/bin/spark-submit app.py \
    --conf spark.ui.prometheus.enabled=true \
    --conf spark.executor.processTreeMetrics.enabled=true \
    --conf spark.metrics.conf=${SPARK_HOME}/conf/metrics.properties
	