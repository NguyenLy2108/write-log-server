import json
from services.post_impression_server import PostImpresstionService
import config

class PostImpressionEvents():
    name = config.Settings().redis_topic
    
    def __init__(self) -> None: 
        self.postImpressionService = PostImpresstionService()
       

    def do(self, message, conn): 
        pre_data = {
            "user_id": message['user_id'],
            "name": message['input']['data']['name'],
            "data": message['input']['data']['message'],
            "ip": message['ip'],
            "timestamp": message['timestamp'],
            "created_at": message['created_at'],
            "query": message['query'],
            "event": message['event']
        }        
       
        self.postImpressionService.create(pre_data, conn)        

    def execute(self, message, conn):
        # Parse Json to Message          
        if message['data'] != 1:            
            data = json.loads(str(message['data']))           
           
            if "event" in data and data['event'] == 'post_impression' and data['user_id'] != 'None': 
                print("---MESSAGE--: ", data)                        
                self.do(data, conn)