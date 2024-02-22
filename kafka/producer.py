import uuid
import simplejson as json
from logger.logger import logs


class Producer:

    def __init__(self):

        self.logger = logs(__name__)

    def json_serializer(self, obj):

        if isinstance(obj, uuid.UUID):
            return str(obj)
        raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

    def delivery_report(self, err, msg):

        if err is not None:
            print(f'Message delivery failed: {err}')
            # self.logger.error(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered successfully to {msg.topic()} [{msg.partition()}]')
            # self.logger.info(f'Message delivered successfully to {msg.topic()} [{msg.partition()}]')

    def produce_data_to_kafka(self, producer, topic, data):

        producer.produce(
            topic,
            key=str(data['id']),
            value=json.dumps(data, default=self.json_serializer).encode('utf-8'),
            on_delivery=self.delivery_report
        )

        producer.flush()
