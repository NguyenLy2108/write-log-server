
import datetime
from listeners.RabbitMQListener import RabbitMQListener
from outputor.FirebasePusher import FirebasePusher
from services.NotificationServer import NotificationService
import pytz

import os
import time

from utils.common import get_device_list_with_notification_config 

# local_tz = pytz.timezone('Asia/Ho_Chi_Minh') # use your local timezone name here

# os.environ['TZ'] = 'Asia/Ho_Chi_Minh'
# time.tzset()
# export enum NotificationEntityType {
#   POST = 1,
#   POST_COMMENT = 2,
#   EVENT = 3,
#   CAST = 4,
#   REVIEW = 5,
#   LIVE_STREAM = 6,

#   EVENT_ADMIN = 7,
#   NEWS_UPGRADE = 8,
# }

class EventNotificationAdminResend():
    name = 'EVENT_NOTIFICATION_ADMIN_RESEND'

    def __init__(self) -> None:
        self.notiService = NotificationService()
        self.firebase = FirebasePusher()
        # self.rabbitmq = RabbitMQListener()
        pass

    def do(self, message):
        print(message)

        data = message["data"]
        event_id = data['event_id']
        
        sql_query = "select user_id from reviewtydev.app_notification where notification_object_id = {} and (is_sent is null or is_sent = 'FAIL')".format(int(data['no_id']))
        
        event_users = self.notiService.query(sql_query)
        if len(event_users) == 0:
            return

        app_notification_object = {
            'entityId': event_id,
            'entityType': 8,
            'receiverIds': [row['user_id'] for row in event_users]
        }
        
        notification_config = 'events'
        if message['type'] == 12:
            notification_config = "news"
            
        device_list = get_device_list_with_notification_config(app_notification_object["receiverIds"], notification_config)
        
        token_list = [r['token'] for r in device_list]
        user_list = [(r['user_id']) for r in device_list]
        
        print("Number of devices: ", len(token_list))

        for idx in range(0, len(token_list), 500):
            send_token_push = {
                'data': {
                    "event_id": str(event_id),
                    "no_id": str(data['no_id']),
                    'type': str(data['type']),
                },
                'type': "send_token_push",
                'tokens': token_list[idx:idx+500],
                'user_ids': user_list[idx:idx+500],
                'no_id': data['no_id'],
            }
            
            self.firebase.publish(send_token_push)
