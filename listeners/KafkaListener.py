
import os
import time 
from kafka import KafkaConsumer

from .ListenerAbstract import ListenerAbstract


class KAFKAConfig():
    KAFKA_HOST: str
    KAFKA_PORT: int
    KAFKA_GROUP: str
    KAFKA_TOPIC: str

class KafkaListener(ListenerAbstract):

    def __init__(self) -> None:
        
        super().__init__()
        self.config = KAFKAConfig(**os.environ)
        self.connect()

    def connect(self):
        self.consumer = KafkaConsumer(self.config.KAFKA_TOPIC, group_id=self.config.KAFKA_GROUP, bootstrap_servers=self.config.KAFKA_TOPIC)
        pass
    
    def start_consumer(self, event_callback):
        print(' [*] KAFKA - Waiting for messages. To exit press CTRL+C')
        for msg in self.consumer:
            event_callback(msg)
            time.sleep(1)

