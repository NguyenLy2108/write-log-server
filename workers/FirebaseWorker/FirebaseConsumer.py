import os
import firebase_admin

from firebase_admin import credentials, messaging

from pydantic import BaseModel
import psycopg2
import psycopg2.extras
import json

from services.NotificationServer import NotificationService 

FIREBASE_JSON_CONFIG = os.environ.get('FIREBASE_JSON_CONFIG', "reviewty-test-firebase-adminsdk-x8m04-eecfc9bbda.json")
firebase_cred = credentials.Certificate(FIREBASE_JSON_CONFIG)
initialize_app = firebase_admin.initialize_app(firebase_cred)


class FirebaseConsumer():
    def __init__(self) -> None:
        self.notiService = NotificationService()

        pass
    
    def execute(self, ch, method, properties, message):
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # Parse Json to Message
        data = json.loads(message.decode('utf-8'))
        
        print('Received a message: ', data['type'])

        self.do(message=data)
    
    def do(self, message):
        no_type = message['type']
        # print(message['data'])
        if no_type ==  "send_token_push":
            return self.send_token_push(data=message['data'], tokens=message['tokens'], user_ids=message['user_ids'],no_id = message['no_id'])
        if no_type ==  "send_topic_android":
            return self.send_topic_android(data=message['data'], topic=message['topic'])
        if no_type ==  "send_topic_ios":
            return self.send_topic_ios(data=message['data'], topic=message['topic'])
        return

    def send_token_push(self, data, tokens, user_ids, no_id):
        print('DATA ', data)
        alert = messaging.ApsAlert(body = " ")
        
        if data.get('entityType') == 11:
            alert = messaging.ApsAlert(body = "Yêu cầu của bạn về sản phẩm đã được xử lý. Cập nhật phiên bản mới để hiển thị chi tiết")
        if data.get('entityType') == 10:
            alert = messaging.ApsAlert(body = "Điểm thưởng của bạn đã thay đổi. Cập nhật phiên bản mới để hiển thị chi tiết")
        if data.get('entityType') == 9:
            alert = messaging.ApsAlert(body = "Bạn được người khác theo dõi. Cập nhật phiên bản mới để hiển thị chi tiết")
        if data.get('entityType') == 8:
            alert = messaging.ApsAlert(body = "Reviewty có tin tức mới. Cập nhật phiên bản mới để hiển thị chi tiết")
        if data.get('entityType') == 7:
            alert = messaging.ApsAlert(body = "Reviewty có sự kiện mới. Cập nhật phiên bản mới để hiển thị chi tiết")


        aps = messaging.Aps(alert = alert, sound = "default", mutable_content=True)
        payload = messaging.APNSPayload(aps)

        for key in data:
            data[key] = str(data[key])

        message = messaging.MulticastMessage(
            data=data,
            tokens=tokens,
            apns = messaging.APNSConfig(payload = payload)
        )

        response = messaging.send_multicast(message)

        successed_user = {}
        failed_user = {}
        failed_tokens = []

        # if response.failure_count > 0:
        responses = response.responses
        for idx, resp in enumerate(responses):
            if not resp.success:
                # The order of responses corresponds to the order of the registration tokens.
                failed_tokens.append(user_ids[idx])
                failed_user[str(user_ids[idx])] = tokens[idx]
            else:
                # print('Success uid', user_ids[idx])
                successed_user[str(user_ids[idx])] = tokens[idx]
                pass

        print('List of users that successed: {0}'.format(len(successed_user)))
        print('List of users that failed_user: {0}'.format(len(failed_user)))
        print('List of users that failed_tokens: {0}'.format(len(failed_tokens)))
           
        if len(failed_user.keys()) > 0:
            data = {
                'is_sent': 'FAIL'
            }
            self.notiService.updateWhere(
                "reviewtydev.app_notification", 
                " notification_object_id = {} and user_id in ({}) ".format(no_id, ','.join([str(uid) for uid in failed_user.keys()])), 
                data
            )

        if len(successed_user.keys()) > 0:
            data = {
                    'is_sent': 'SUCCESS'
            }
            self.notiService.updateWhere(
                "reviewtydev.app_notification", 
                " notification_object_id = {} and user_id in ({}) ".format(no_id, ','.join([str(uid) for uid in successed_user.keys()])), 
                data
            )

    def update_notification_status(self):
        pass

    def send_topic_android(self, data, topic):
        message = messaging.Message(
            data=data,
            topic=topic,
        )

        response = messaging.send(message)

    def send_topic_ios(self, data, topic):
        alert = messaging.ApsAlert(title = data['title'], body = data['content'])
        aps = messaging.Aps(alert = alert, sound = "default")
        payload = messaging.APNSPayload(aps)

        message = messaging.Message(
            notification=messaging.Notification(
                title=data['title'],
                body=data['content'],
            ),
            data=data,
            topic=topic,    
            apns = messaging.APNSConfig(payload = payload)
        )

        # Send a message to the devices subscribed to the provided topic.
        response = messaging.send(message)
        print('Response IOS', response)

# Test
if __name__ == '__main__':
    message = {'data': {'type': 27, 'entityId': 9301355, 'entityType': 10, 'senderId': 2556, 'senderName': 'Reviewty_Official', 'point': 2}, 'type': 'send_token_push', 'tokens': ['dMPiO6M-gUSavEu9uQLBhQ:APA91bHbESyRoDwT0WrHZgcZwU4VoJnB953Tsrh8hOWF13QaJMDweS2vEESdz4wfGUp3OnkJwaC6QvdvR8YhpxuZ5JCWsN9xhnZeVBNKS1n_L2iS_eo7_pYpI8h4jr5f62WaQ5rE-vT1'], 'user_ids': [921341], 'no_id': 2395540}
    pusher = FirebaseConsumer()
    pusher.do(message)