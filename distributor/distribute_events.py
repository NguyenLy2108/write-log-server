# import listener
# import worker 
# for a listener has events, distribute them to the related worker
import json
import os 

from pydantic import BaseModel

from listeners.RabbitMQListener import RabbitMQListener
from events.EventFactory import EventFactory

class DistributorConfig(BaseModel):
    RABBIT_TOPIC: str = "EVENT_PROCESSOR_DEV"

class Distributor():
    
    def __init__(self):
        if len(os.environ) > 0:
            self.config = DistributorConfig(**os.environ)
        else:
            self.config = DistributorConfig()

        self.factories = EventFactory()
        self.listener = RabbitMQListener()
        pass

    def distribute(self, ch, method, properties, message):
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # Parse Json to Message
        data = json.loads(message.decode('utf-8'))
        print('Received a message: ', data)

        event = self.factories.getEvent(data['type'])
        
        if event:
            event.do(message=data)
        else:
            print('Not Found', data['type'])
    
    def listen(self):
        self.listener.start_consumer(self.config.RABBIT_TOPIC, self.distribute)

    