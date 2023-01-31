
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

class EventNotificationAdmin():
    name = 'NEW_EVENT_NOTIFICATION_ADMIN'

    def __init__(self) -> None:
        self.notiService = NotificationService()
        self.firebase = FirebasePusher()
        self.rabbitmq = RabbitMQListener()
        pass

    def do(self, message):

        sql_query = "select * from reviewtydev.notification_event where id = '{}'".format(message['data']['event_id'])
        
        notification = self.notiService.query(sql_query)[0]
        
        if 'mode' in message['data'] and message['data']['mode'] == 'alarm' and (notification.get('alarm_time', None) != None) :

            if int(notification['alarm_time'].strftime('%s')) <= int(time.time()):
                self.process_message(message)
            else:
                self.rabbitmq.publish(os.environ.get("RABBIT_TOPIC", 'EVENT_PROCESSOR_DEV'), message)
                time.sleep(2)
        else:
            self.process_message(message)
    
    def process_message(self, message):
        message = message["data"]
        
        event_id = message['event_id']
        
        sql_query = "select title, attached_link, content from reviewtydev.notification_event where id = '{}'".format(event_id)
        
        event = self.notiService.query(sql_query)[0]

        sql_query = "select user_id from reviewtydev.notification_event_user where event_id = '{}'".format(event_id)
        
        event_users = self.notiService.query(sql_query)
        if len(event_users) == 0:
            print('Receiver is empty')
            return

        app_notification_object = {
            'entityId': event_id,
            'entityType': message['entity_type'],
            'type': message['type'],
            'receiverIds': [row['user_id'] for row in event_users],
            'changerIds': [2556], # Default sender is the reviewty official
        }
        
        noId = self.notiService.create(app_notification_object)
        
        self.notiService.update("reviewtydev.notification_event", {"id": int(event_id)} , {"no_id": noId, "status": 1})

        notification_config = 'events'
        if message['type'] == 12:
            notification_config = "news"

        device_list = get_device_list_with_notification_config(app_notification_object["receiverIds"], notification_config)
        
        token_list = [r['token'] for r in device_list]
        user_list = [(r['user_id']) for r in device_list]
        
        print("Number of devices: ", len(token_list))
        if event.get('attached_link', '') == None:
            event['attached_link'] = ''
            
        for idx in range(0, len(token_list), 500):
            send_token_push = {
                'data': {
                    "event_id": str(event_id),
                    "no_id": str(noId),
                    'type': str(message['type']),
                    'title': event['title'],
                    'attached_link': event.get('attached_link', ''),
                    "body": event.get('content', '')
                },
                'type': "send_token_push",
                'tokens': token_list[idx:idx+500],
                'user_ids': user_list[idx:idx+500],
                'no_id': noId,
            }
            
            self.firebase.publish(send_token_push)

