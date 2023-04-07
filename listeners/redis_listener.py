import time
import redis
import config
import os

from .listener_abstract import ListenerAbstract

class RedisListener(ListenerAbstract):   
    def __init__(self) -> None:
        super().__init__()
        if len(os.environ) > 0:
            self.config = config.Settings(**os.environ)
        else:
            self.config = config.Settings()

        self.connect()

    def connect(self):  
        self.connection = redis.Redis(
            host=self.config.redis_host,
            port=self.config.redis_port,
            decode_responses=True
        )  
            
    def start_consumer(self, redis_topic, event_callback):    
        self.consumer = self.connection.pubsub()    
        self.consumer.subscribe(redis_topic)  
        self.consumer.listen()        
        for msg in self.consumer.listen():
            event_callback(msg, self.connection)
            time.sleep(1)

    