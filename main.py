import os
import time
import threading
from confluent_kafka import SerializingProducer

from app.iot.gps_data import GPSData
from app.iot.vehicle_data import VehicleData
from app.iot.traffic_data import TrafficData
from app.iot.weather_data import WeatherData
from app.iot.emergency_incident_data import EmergencyIncidentData

from app.kafka.producer import Producer

kafka_bootstrap_servers = os.getenv('KAFKA_BOOSTRAP_SERVERS', 'localhost:9092')

gps_topic = os.getenv('GPS_TOPIC', 'gps_data')
traffic_topic = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
weather_topic = os.getenv('WEATHER_TOPIC', 'weather_data')
vehicle_topic = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
emergency_topic = os.getenv('EMERGENCY_TOPIC', 'emergency_data')


class VehicleMovement:

    def __init__(self):

        self.gps_data = GPSData()
        self.vehicle_data = VehicleData()
        self.traffic_data = TrafficData()
        self.weather_data = WeatherData()
        self.emergency_incident_data = EmergencyIncidentData()

        self.producer = Producer()

    def simulate_vehicle_movement(self, producer, device_id):

        while True:
            vehicle_data = self.vehicle_data.generate_vehicle_data(device_id)
            gps_data = self.gps_data.generate_gps_data(device_id, vehicle_data['timestamp'])
            traffic_camera_data = self.traffic_data.generate_traffic_camera_data(
                device_id, vehicle_data['timestamp'], vehicle_data['location'], 'camera-1')
            weather_data = self.weather_data.get_weather_data(
                device_id, vehicle_data['timestamp'], vehicle_data['location'])
            emergency_incident_data = self.emergency_incident_data.get_emergency_incident_data(
                device_id, vehicle_data['timestamp'], vehicle_data['location'])

            if (vehicle_data['location'][0] >= self.vehicle_data.birmingham_coordinates['latitude'] and
                    vehicle_data['location'][1] <= self.vehicle_data.birmingham_coordinates['longitude']):
                print('Vehicle has reached Birmingham. Stopping the simulation')

                break

            self.producer.produce_data_to_kafka(producer, gps_topic, gps_data)
            self.producer.produce_data_to_kafka(producer, vehicle_topic, vehicle_data)
            self.producer.produce_data_to_kafka(producer, weather_topic, weather_data)
            self.producer.produce_data_to_kafka(producer, traffic_topic, traffic_camera_data)
            self.producer.produce_data_to_kafka(producer, emergency_topic, emergency_incident_data)

            time.sleep(10)

    def produce_data_to_kafka(self):

        producer_config = {
            'bootstrap.servers': kafka_bootstrap_servers,
            'error_cb': lambda err: print(f'Error: {err}'),
        }

        producer = SerializingProducer(producer_config)

        try:
            self.simulate_vehicle_movement(producer, 'Smart-Car-1')
        except KeyboardInterrupt:
            print('Simulation ended by the user')
            pass
        except Exception as e:
            print(f'An error occurred: {e}')

    @staticmethod
    def spark_submit():

        os.system('./submit_spark.sh')


if __name__ == "__main__":
    vehicle_movement = VehicleMovement()

    # Create threads for running produce_data_to_kafka and spark_submit concurrently
    kafka_thread = threading.Thread(target=vehicle_movement.produce_data_to_kafka)
    spark_thread = threading.Thread(target=vehicle_movement.spark_submit)

    # Start the threads
    kafka_thread.start()
    time.sleep(10)
    spark_thread.start()

    # Wait for both threads to finish
    kafka_thread.join()
    spark_thread.join()
