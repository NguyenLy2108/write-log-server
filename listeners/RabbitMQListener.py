import json
import pika
import os

from pydantic import BaseModel

from .ListenerAbstract import ListenerAbstract

class RabbitConfig(BaseModel):
    RABBIT_HOST: str = 'localhost'
    RABBIT_PORT: int = 5672
    RABBIT_USER: str = 'reviewty'
    RABBIT_PWD: str = 'a1b2c3'
    # RABBIT_TOPIC: str = 'EVENT_PROCESSOR'

class RabbitMQListener(ListenerAbstract):
    def __init__(self) -> None:
        super().__init__()
        if len(os.environ) > 0:
            self.config = RabbitConfig(**os.environ)
        else:
            self.config = RabbitConfig()

        self.connect()

    def connect(self):
        self.credentials = pika.PlainCredentials(self.config.RABBIT_USER, self.config.RABBIT_PWD)
        self.parameters = pika.ConnectionParameters(self.config.RABBIT_HOST,
                                        self.config.RABBIT_PORT,
                                        '/',
                                        self.credentials,
                                        heartbeat=0
                                        )

        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()


    def start_consumer(self, RABBIT_TOPIC, event_callback):
        print(self.config, RABBIT_TOPIC)
        self.channel.queue_declare(queue=RABBIT_TOPIC)

        self.channel.basic_qos(prefetch_count=1)

        self.channel.basic_consume(queue=RABBIT_TOPIC, on_message_callback=event_callback, auto_ack=False)

        print(' [*] RABBITMQ - Waiting for messages. To exit press CTRL+C')
        
        self.channel.start_consuming()
    
    def publish(self, RABBIT_TOPIC, data):
        channel = self.connection.channel()
        channel.queue_declare(queue=RABBIT_TOPIC)

        channel.basic_publish(exchange='', routing_key=RABBIT_TOPIC, body=json.dumps(data,ensure_ascii=False).encode('utf-8'))

