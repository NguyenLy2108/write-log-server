import os
import firebase_admin

from firebase_admin import credentials, messaging

from pydantic import BaseModel
import psycopg2
import psycopg2.extras

from listeners.RabbitMQListener import RabbitMQListener

FIREBASE_JSON_CONFIG = os.environ.get('FIREBASE_JSON_CONFIG', "reviewty-test-firebase-adminsdk-x8m04-eecfc9bbda.json")
firebase_cred = credentials.Certificate(FIREBASE_JSON_CONFIG)
initialize_app = firebase_admin.initialize_app(firebase_cred)

class FirebaseConfig(BaseModel):
    FIREBASE_WORKER_TOPIC: str = 'FIREBASE_WORKER_DEV'

class FirebasePusher():
    def __init__(self) -> None:
        self.pusher = RabbitMQListener()
        if len(os.environ) > 0:
            self.config = FirebaseConfig(**os.environ)
        else:
            self.config = FirebaseConfig()
        pass

    def publish(self, notification_data):
        self.pusher.publish(self.config.FIREBASE_WORKER_TOPIC, notification_data)
