# worker

# include listener and event, do event then sending to outputor if nessesary
# connect to a listener and 
# import listener
# import worker 
# for a listener has events, distribute them to the related worker
import json

from listeners.redis_listener import RedisListener
from events.create_post_impresstion_event import PostImpressionEvents

class PostImpressionEventWorker():
    
    def __init__(self):
        self.worker = PostImpressionEvents()
        self.listener = RedisListener()
        pass
    
    def listen(self):
        print('Worker start listening')
        self.listener.start_consumer(self.worker.name, self.worker.execute)    

