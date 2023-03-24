# worker

# include listener and event, do event then sending to outputor if nessesary
# connect to a listener and 
# import listener
# import worker 
# for a listener has events, distribute them to the related worker
import json

from listeners.RabbitMQListener import RabbitMQListener
from events.CreatePostEvent import PostEvents

class PostEventWorker():
    
    def __init__(self):
        self.worker = PostEvents()
        self.listener = RabbitMQListener()
        pass
    
    def listen(self):
        self.listener.start_consumer(self.worker.name, self.worker.execute)

if __name__ == "__main__":
    worker = PostEventWorker()
    worker.listen()