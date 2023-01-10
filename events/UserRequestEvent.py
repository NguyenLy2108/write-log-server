import json
from database.database import PostgresqlStorage
from services.NotificationServer import NotificationService
from outputor.FirebasePusher import FirebasePusher
import time

from utils.common import get_device_list_with_notification_config 

class UserRequestEvent():
    name = 'USER_REQUEST_EVENT'
    
    def __init__(self) -> None: 
        self.notiService = NotificationService()
        self.firebase = FirebasePusher()
    
    def do(self, message):

        data = message['data']

        sql_query = "select id, owner_id, owner_id, requested_object_id, assigned_ids_product_by_admin_for_user from reviewtydev.user_request where id in ({})".format(','.join([str(id) for id in data['entity_id']['ids']]))
        
        user_requests_list = self.notiService.query(sql_query)

        for request in user_requests_list:
            user_id = request['owner_id']
            product_id = request['requested_object_id']

            try:
                if product_id == None:
                    product_id = request['assigned_ids_product_by_admin_for_user'][0]
            except:
                pass
            
            if product_id == None:
                return

            sql_query = "select name from reviewtydev.product_content where product_id = {}".format(product_id)

            product_info = self.notiService.query(sql_query)[0]

            app_notification_object = {
                'entityId': product_id,
                'entityType': data['entity_type'],
                'type': data['type'],
                'receiverIds': [user_id],
                'changerIds': [2556], # Default sender is the reviewty official
            }

            noId = self.notiService.create(app_notification_object)

            self.notiService.update("reviewtydev.user_request", {"id": request['id']} , {"status_send_notification": 4})

            device_list = get_device_list_with_notification_config(app_notification_object["receiverIds"], 'news')

            token_list = [r['token'] for r in device_list]
            user_list = [(r['user_id']) for r in device_list]
            
            print("Number of devices: ", len(token_list))

            for idx in range(0, len(token_list), 500):
                send_token_push = {
                    'data': {
                        'type': data['type'],
                        'entityType': data['entity_type'],
                        'productId': product_id,
                        'productName': product_info['name'],
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

