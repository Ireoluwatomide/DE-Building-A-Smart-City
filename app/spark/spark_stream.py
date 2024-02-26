from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField, TimestampType, DoubleType, IntegerType

from app.spark.spark_session import SparkSessionManager


class SparkStream:

    def __init__(self):
        self.spark = SparkSessionManager().get_spark_session()

    def read_kafka_stream(self, topic, schema):
        read_stream = (self.spark.readStream
                       .format('kafka')
                       .option('kafka.bootstrap.servers', 'localhost:9092')
                       .option('subscribe', topic)
                       .option('startingOffsets', 'earliest')
                       .load()
                       .selectExpr('CAST(value AS STRING)')
                       .select(from_json(col('value'), schema).alias('data'))
                       .select('data.*')
                       .withWatermark('timestamp', '1 minutes')
                       )

        return read_stream

    @staticmethod
    def stream_writer(df: DataFrame, checkpoint_folder, output):
        write_stream = (df.writeStream
                        .format('parquet')
                        .option('checkpointLocation', checkpoint_folder)
                        .option('path', output)
                        .outputMode('append')
                        .start()
                        )

        return write_stream

    def process_streams(self):
        vehicle_schema = StructType([
            StructField('id', StringType(), True),
            StructField('device_id', StringType(), True),
            StructField('timestamp', TimestampType(), True),
            StructField('location', StringType(), True),
            StructField('speed', DoubleType(), True),
            StructField('direction', StringType(), True),
            StructField('manufacturer', StringType(), True),
            StructField('model', StringType(), True),
            StructField('year', IntegerType(), True),
            StructField('fuel_type', StringType(), True),
        ])

        gps_schema = StructType([
            StructField('id', StringType(), True),
            StructField('device_id', StringType(), True),
            StructField('timestamp', TimestampType(), True),
            StructField('speed', DoubleType(), True),
            StructField('direction', StringType(), True),
            StructField('vehicle_type', StringType(), True),
        ])

        traffic_schema = StructType([
            StructField('id', StringType(), True),
            StructField('device_id', StringType(), True),
            StructField('camera_id', StringType(), True),
            StructField('location', StringType(), True),
            StructField('timestamp', TimestampType(), True),
            StructField('snapshot', StringType(), True),
        ])

        weather_schema = StructType([
            StructField('id', StringType(), True),
            StructField('device_id', StringType(), True),
            StructField('location', StringType(), True),
            StructField('timestamp', TimestampType(), True),
            StructField('temperature', DoubleType(), True),
            StructField('weather_condition', StringType(), True),
            StructField('precipitation', DoubleType(), True),
            StructField('wind_speed', DoubleType(), True),
            StructField('humidity', IntegerType(), True),
            StructField('air_quality_index', DoubleType(), True),
        ])

        emergency_schema = StructType([
            StructField('id', StringType(), True),
            StructField('device_id', StringType(), True),
            StructField('incident_id', StringType(), True),
            StructField('type', StringType(), True),
            StructField('timestamp', TimestampType(), True),
            StructField('location', StringType(), True),
            StructField('status', StringType(), True),
            StructField('description', StringType(), True),
        ])

        gps_df = self.read_kafka_stream('gps_data', gps_schema).alias('gps')
        vehicle_df = self.read_kafka_stream('vehicle_data', vehicle_schema).alias('vehicle')
        traffic_df = self.read_kafka_stream('traffic_data', traffic_schema).alias('traffic')
        weather_df = self.read_kafka_stream('weather_data', weather_schema).alias('weather')
        emergency_df = self.read_kafka_stream('emergency_data', emergency_schema).alias('emergency')

        query1 = self.stream_writer(gps_df, 's3a://de-smart-city/_checkpoints/gps_data',
                                    's3a://de-smart-city/data/gps_data')
        query2 = self.stream_writer(vehicle_df, 's3a://de-smart-city/_checkpoints/vehicle_data',
                                    's3a://de-smart-city/data/vehicle_data')
        query3 = self.stream_writer(traffic_df, 's3a://de-smart-city/_checkpoints/traffic_data',
                                    's3a://de-smart-city/data/traffic_data')
        query4 = self.stream_writer(weather_df, 's3a://de-smart-city/_checkpoints/weather_data',
                                    's3a://de-smart-city/data/weather_data')
        query5 = self.stream_writer(emergency_df, 's3a://de-smart-city/_checkpoints/emergency_data',
                                    's3a://de-smart-city/data/emergency_data')

        query5.awaitTermination()


if __name__ == '__main__':
    streaming = SparkStream()
    streaming.process_streams()
