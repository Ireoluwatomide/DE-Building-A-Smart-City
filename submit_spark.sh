export PYTHONPATH="${PYTHONPATH}:$(pwd)"
spark-submit \
  --master spark://Marvellouss-Air:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469 \
  ./app/spark/spark_stream.py