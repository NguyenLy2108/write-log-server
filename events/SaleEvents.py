
import datetime
from listeners.RabbitMQListener import RabbitMQListener
from outputor.FirebasePusher import FirebasePusher
from services.NotificationServer import NotificationService
import pytz

import os
import time 

# local_tz = pytz.timezone('Asia/Ho_Chi_Minh') # use your local timezone name here

# os.environ['TZ'] = 'Asia/Ho_Chi_Minh'
# time.tzset()


class SaleEvents():
    name = 'SALE_EVENT_NOTIFICATION'

    def __init__(self) -> None:
        self.notiService = NotificationService()
        self.firebase = FirebasePusher()
        self.rabbitmq = RabbitMQListener()
        pass

    def do(self, message):

        sql_query = "select * from reviewtydev.run_event_history where noti_id = '{}' and status = false".format(message['data']['noti_id'])
        
        notification = self.notiService.query(sql_query)

        for data in notification:

            if int(data['notified_at'].strftime('%s')) <= int(time.time()):
                self.send_event_notification(message)
                pass
            else:
                self.rabbitmq.publish(os.environ.get("RABBIT_TOPIC", 'EVENT_PROCESSOR_DEV'), message)
                time.sleep(2)

        return
    
    def send_event_notification(self, message):
        
        sql_query = "select thumbnail from reviewtydev.event where id = {}".format(message['data']['event_id'])
        
        event = self.notiService.query(sql_query)

        thumbnail = event[0]['thumbnail']

        noti_data = {
            "item_id": str(message['data']['event_id']),
            "type": 'event',
            "content": message['data']['content'],
            "title": message['data']['title'],
            "image_url": thumbnail,
        }
            
        self.firebase.publish({
                'data': noti_data,
                'type': "send_topic_android",
                'topic': "/topics/event",
        })

        self.firebase.publish({
                'data': noti_data,
                'type': "send_topic_ios",
                'topic': "/topics/event-ios",
        })

        # self.firebase.send_topic(noti_data, "/topics/event")
        # self.firebase.send_topic_ios(noti_data, "/topics/event-ios")