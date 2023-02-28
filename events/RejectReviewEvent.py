import json
from database.database import PostgresqlStorage
from services.NotificationServer import NotificationService
from outputor.FirebasePusher import FirebasePusher
import time

from utils.common import get_device_list_with_notification_config 

class RejectReviewEvents():
    name = 'REJECT_REVIEW'
    
    def __init__(self) -> None: 
        self.notiService = NotificationService()
        self.firebase = FirebasePusher()

    def do(self, message):
        message = message["data"]

        review = self.get_review(message['entity_id'])
        author_id = review['author_id']
        product_id = review['product_id']

        app_notification_object = {
            'entityId': message['entity_id'],
            'entityType': message['entity_type'],
            'type': message['type'],
            'receiverIds': [author_id],
            'changerIds': [2556], # Default sender is the reviewty official
        }

        noId = self.notiService.create(app_notification_object)

        data = {
            'entityId': message['entity_id'],
            'entityType': message['entity_type'],
            'type': message['type'],
            'productId': product_id,
            'no_id': noId,
            'title': 'Cập nhật phiên bản ứng dụng để xem nội dung mới nhất',
        }

        user_info = self.get_users(2556)[0]
        data['account'] = user_info['account_name']
        
        try:
            data['avatar'] = self.get_avatar(user_info['avatar_id'])
        except:
            data['avatar'] = ''

        device_list = get_device_list_with_notification_config([author_id], 'activity')

        self.send_notification(device_list, data, noId=noId)


    def get_review(self, reviewId):
        sql_query = f'select r.author_id, r.product_id from reviewtydev.real_review r where r.id={reviewId}'
        return self.notiService.query(sql_query)[0]


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
    

    def send_notification(self, receiver_token_list, data, noId):
        
        print("Start sending message to the Firebase")
        
        start = time.time()
        for key in data.keys():
            data[key] = str(data[key])
        
        print('Firebase Data: ', data)

        token_list = [r['token'] for r in receiver_token_list]
        user_list = [r['user_id'] for r in receiver_token_list]

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

