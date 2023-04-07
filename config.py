import os
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()

class Settings(BaseModel):    
    redis_host = os.getenv('REDIS_HOST')    
    redis_port = os.getenv('REDIS_PORT')
    redis_topic = os.getenv('REDIS_TOPIC')
    redis_pub_topic = os.getenv('REDIS_PUB_TOPIC')

    postgre_host = os.getenv('POSTGRE_HOST')    
    postgre_port = os.getenv('POSTGRE_PORT')
    postgre_database = os.getenv('POSTGRE_DATABASE')
    postgre_user = os.getenv('POSTGRE_USER')    
    postgre_password = os.getenv('POSTGRE_PASSWORD') 