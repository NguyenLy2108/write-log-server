import json
from database.database import PostgresqlStorage
from services.NotificationServer import NotificationService
from outputor.FirebasePusher import FirebasePusher
import time

from utils.common import get_device_list_with_notification_config, get_users, get_avatar 

class ReactPostEvents():
    name = 'REACT_POST_NOTIFICATION'
    
    def __init__(self) -> None: 
        self.notiService = NotificationService()
        self.firebase = FirebasePusher()
    
    def do(self, message):
        data = message['data']
        # Insert notification information to the database
        sql_query = "select user_id from reviewtydev.post where id = {0} and user_id not in (select user_id from reviewtydev.user_to_unsubscribed_post utup where post_id = {0})"
        
        sql_query = sql_query.format(data['entityId'])

        postAuthor = self.notiService.query(sql_query)
        print(sql_query, postAuthor)

        postAuthorId = postAuthor[0]['user_id']
        if (postAuthorId == data['currentUserId']):
            print("Same person")
            return

        data['receiverIds'] = [postAuthorId]
        
        noId = self.notiService.create(data=data)

        if noId:
            user_info = get_users(data['currentUserId'])[0]
            try:
                avatar = get_avatar(user_info['avatar_id'])
            except:
                avatar = ''

            send_data =  {
                   "type": data['type'],
                   "account": data['account'],
                   "avatar": avatar,
                   "notificationId": noId,
                   "postId": data['entityId'],
                   "status": data['status'] ,
                   "no_id": str(noId),
            }

            device_list = get_device_list_with_notification_config([postAuthorId], 'activity')

            self.send_notification(device_list, send_data, noId=noId)

        
    def send_notification(self, follower_token_list, data, noId):
        print("Start sending message to the Firebase")
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

        print("Send to Firebase: DONE")

