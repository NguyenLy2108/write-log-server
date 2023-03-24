import json
from database.database import PostgresqlStorage
from services.NotificationServer import NotificationService
from outputor.FirebasePusher import FirebasePusher
import time

from utils.common import get_device_list_with_notification_config, get_avatar, get_users

class FollowUserEvent():
    name = 'FOLLOW_USER_EVENT'
    
    def __init__(self) -> None: 
        self.notiService = NotificationService()
        self.firebase = FirebasePusher()
    
    def do(self, message):
        data = message['data']
        
        app_notification_object = {
            'entityId': data['follower_id'],
            'entityType': data['entity_type'],
            'type': data['type'],
            'receiverIds': [data["user_id"]],
            'changerIds': [data["follower_id"]],
        }

        noId = self.notiService.create(app_notification_object)

        device_list = get_device_list_with_notification_config([data["user_id"]], 'activity')
        
        token_list = [r['token'] for r in device_list]
        user_list = [(r['user_id']) for r in device_list]
        
        print("Number of devices: ", len(token_list))
        user_info = get_users(data['follower_id'])[0]
        follower_name = user_info['account_name']

        try:
            avatar = get_avatar(user_info['avatar_id'])
        except:
            avatar = ''

        for idx in range(0, len(token_list), 500):
            send_token_push = {
                'data': {
                    'type': data['type'],
                    'entityId': data['follower_id'],
                    'entityType': data['entity_type'],
                    'followerName': follower_name,
                    'avatar': avatar,
                    'no_id': noId,
                },
                'type': "send_token_push",
                'tokens': token_list[idx:idx+500],
                'user_ids': user_list[idx:idx+500],
                'no_id': noId,
            }
            
            self.firebase.publish(send_token_push)

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

        print("Send to Firebase - DONE in time:", time.time() - start)

