import uuid
import random

from logger.logger import logs


class EmergencyIncidentData:

    def __init__(self):

        self.logger = logs(__name__)

    @staticmethod
    def get_emergency_incident_data(device_id, timestamp, location):

        emergency_incident_data = {
            'id': uuid.uuid4(),
            'device_id': device_id,
            'incident_id': uuid.uuid4(),
            'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
            'timestamp': timestamp,
            'location': location,
            'status': random.choice(['Active', 'Resolved']),
            'description': 'Description of the incident'
        }

        # self.logger.info(emergency_incident_data)

        return emergency_incident_data
