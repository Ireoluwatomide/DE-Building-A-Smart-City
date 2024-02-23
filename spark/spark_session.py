from pyspark.sql import SparkSession
from ..config.aws_config import configuration


class SparkSessionManager:

    def __init__(self, app_name='Smart City Streaming'):

        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config('spark.jars.packages',
                    'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,'
                    'org.apache.hadoop:hadoop-aws:3.3.1,'
                    'com.amazonaws:aws-java-sdk:1.11.469') \
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
            .config('spark.hadoop.fs.s3a.access.key', configuration.get('AWS_ACCESS_KEY')) \
            .config('spark.hadoop.fs.s3a.secret.key', configuration.get('AWS_SECRET_KEY')) \
            .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                    'org.apache.hadoop.fs.s3a.impl.SimpleAWSCredentailsProvider') \
            .config('spark.log.level', 'WARN') \
            .getOrCreate()

    def get_spark_session(self):

        return self.spark

    def stop_spark_session(self):
        if self.spark:
            self.spark.stop()
            print("Spark session stopped.")
