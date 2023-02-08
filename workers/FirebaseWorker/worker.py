import json
import os 
from listeners.RabbitMQListener import RabbitMQListener
from workers.FirebaseWorker.FirebaseConsumer import FirebaseConsumer

class Worker():
    
    def __init__(self):
        self.worker = FirebaseConsumer()
        self.listener = RabbitMQListener()
        pass
    
    def listen(self):
        self.listener.start_consumer(os.environ.get('FIREBASE_WORKER_TOPIC', 'FIREBASE_WORKER_DEV'), self.worker.execute)

if __name__ == "__main__":
    worker = Worker()
    worker.listen()