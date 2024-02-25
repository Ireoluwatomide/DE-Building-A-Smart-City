import uuid

from app.logger.logger import logs


class TrafficData:

    def __init__(self):

        self.logger = logs(__name__)

    @staticmethod
    def generate_traffic_camera_data(device_id, timestamp, location, camera_id):

        traffic_camera_data = {
            'id': uuid.uuid4(),
            'device_id': device_id,
            'camera_id': camera_id,
            'location': location,
            'timestamp': timestamp,
            'snapshot': 'Base64EncodedString'
        }

        # self.logger.info(traffic_camera_data)

        return traffic_camera_data
