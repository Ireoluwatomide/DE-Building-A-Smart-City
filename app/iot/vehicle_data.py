import uuid
import random
from datetime import datetime, timedelta

from app.logger.logger import logs


class VehicleData:

    def __init__(self):

        self.logger = logs(__name__)

        self.london_coordinates = {
            "latitude": 51.5074,
            "longitude": -0.1278
        }
        self.birmingham_coordinates = {
            "latitude": 52.4862,
            "longitude": -1.8904
        }

        self.start_time = datetime.now()
        self.start_location = self.london_coordinates.copy()

        self.latitude_increment = (self.birmingham_coordinates['latitude'] - self.london_coordinates['latitude'])/100
        self.longitude_increment = (self.birmingham_coordinates['longitude'] - self.london_coordinates['longitude'])/100

    def get_next_time(self):

        self.start_time += timedelta(seconds=random.randint(30, 60))

        return self.start_time

    def simulate_vehicle_movement(self):

        self.start_location['latitude'] += self.latitude_increment
        self.start_location['longitude'] += self.longitude_increment

        self.start_location['latitude'] += random.uniform(-0.0005, 0.0005)
        self.start_location['longitude'] += random.uniform(-0.0005, 0.0005)

        # self.logger.info(json.dumps(self.start_location))

        return self.start_location

    def generate_vehicle_data(self, device_id):

        location = self.simulate_vehicle_movement()

        vehicle_data = {
            'id': uuid.uuid4(),
            'device_id': device_id,
            'timestamp': self.get_next_time().isoformat(),
            'location': (location['latitude'], location['longitude']),
            'speed': random.uniform(10, 40),
            'direction': 'North-East',
            'manufacturer': 'Toyota',
            'model': 'Prius',
            'year': '2018',
            'fuel_type': 'Hybrid',
        }

        # self.logger.info(vehicle_data)

        return vehicle_data
