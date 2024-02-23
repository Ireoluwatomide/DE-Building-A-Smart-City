import uuid
import random
from logger.logger import logs


class GPSData:

    def __init__(self):

        self.logger = logs(__name__)

    @staticmethod
    def generate_gps_data(device_id, timestamp, vehicle_type='private'):

        gps_data = {
            'id': uuid.uuid4(),
            'device_id': device_id,
            'timestamp': timestamp,
            'speed': random.uniform(0, 40),
            'direction': 'North-East',
            'vehicle_type': vehicle_type,
        }

        # self.logger.info(gps_data)

        return gps_data
