import uuid
import random

from logger.logger import logs

random.seed(42)


class WeatherData:

    def __init__(self):

        self.logger = logs(__name__)

    def get_weather_data(self, device_id, timestamp, location):

        weather_data = {
            'id': uuid.uuid4(),
            'device_id': device_id,
            'location': location,
            'timestamp': timestamp,
            'temperature': random.uniform(-5, 26),
            'weather_condition': random.choice(['Sunny', 'Cloudy', 'Rainy', 'Snowy']),
            'precipitation': random.uniform(0, 25),
            'wind_speed': random.uniform(0, 100),
            'humidity': random.randint(0, 100),  # percentage
            'air_quality_index': random.uniform(0, 500)
        }

        # self.logger.info(weather_data)

        return weather_data
