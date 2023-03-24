import json
from database.database import PostgresqlStorage
from services.NotificationServer import NotificationService
from outputor.FirebasePusher import FirebasePusher
import time

from utils.common import get_device_list_with_notification_config 

class CreatePostEvents():
    name = 'CREATE_POST_NOTIFICATION'
    
    def __init__(self) -> None: 
        self.notiService = NotificationService()
        self.firebase = FirebasePusher()

    def do(self, message):
        
        # Insert notification information to the database
        follower_ids = self.get_follower_ids(message['data'])

        message['noti']['receiverIds'] = follower_ids
        
        if len(follower_ids) == 0:
            return

        noId = self.notiService.create(message['noti'])
        
        data = message['data']

        data['notificationId'] = noId
        
        user_info = self.get_users(data['authorId'])[0]
        data['account'] = user_info['account_name']
        
        try:
            data['avatar'] = self.get_avatar(user_info['avatar_id'])
        except:
            data['avatar'] = ''

        receiverIds = self.get_follower_ids(data)

        device_list = get_device_list_with_notification_config(receiverIds, 'activity')

        print("Nb followers: ", len(device_list))

        self.send_notification(device_list, data, noId=noId)


    def execute(self, ch, method, properties, message):
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # Parse Json to Message
        data = json.loads(message.decode('utf-8'))
        
        self.do(data)
        

    def get_users(self, user_id = ''):
        sql_query = f'select u.account_name , u.avatar_id from reviewtydev.user u where u.id={user_id}'
        return self.notiService.query(sql_query)
    
    def get_avatar(self, image_id):
        sql_query = f'select url from reviewtydev.image where id={image_id}'
        return self.notiService.query(sql_query)[0]['url']
    
    def get_follower_ids(self, data):
        user_id = data['authorId']
        
        sql_query = f"select follower_id from reviewtydev.user_to_follower utf2 where user_id={user_id}"
        
        follower_token_list = self.notiService.query(sql_query)
        
        return [r['follower_id'] for r in follower_token_list]

    def get_follower_token(self, data):
        user_id = data['authorId']
        
        sql_query = f"select token, user_id from reviewtydev.device d where d.user_id in (select follower_id from reviewtydev.user_to_follower utf2 where user_id={user_id})"
        
        follower_token_list = self.notiService.query(sql_query)
        
        return follower_token_list

    def send_notification(self, follower_token_list, data, noId):
        
        print("Start sending message to the Firebase")
        
        start = time.time()
        for key in data.keys():
            data[key] = str(data[key])
        
        print('Firebase Data: ', data)

        token_list = [r['token'] for r in follower_token_list]
        user_list = [r['user_id'] for r in follower_token_list]

        for idx in range(0, len(token_list), 500):
            send_token_push = {
                'data': data,
                'type': "send_token_push",
                'tokens': token_list[idx:idx+500],
                'user_ids': user_list[idx:idx+500],
                'no_id': noId,
            }
            
            self.firebase.publish(send_token_push)
            
            # self.firebase.send_token_push(data, follower_token_list[idx:idx+500])

        print("Send to Firebase: DONE", time.time() - start)

