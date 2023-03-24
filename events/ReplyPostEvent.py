import json
from database.database import PostgresqlStorage
from services.NotificationServer import NotificationService
from outputor.FirebasePusher import FirebasePusher
import time

from utils.common import get_device_list_with_notification_config, get_users, get_avatar 

class ReplyPostEvents():
    name = 'REPLY_POST_NOTIFICATION'
    
    def __init__(self) -> None: 
        self.notiService = NotificationService()
        self.firebase = FirebasePusher()
    
    def do(self, message):
        print(message)
        message = message['data']
        comment_data = message['commentData']
        
        owner_id =  -1
        noti_type = 0
        receiverIds = []
        
        if not comment_data['parentId']:
            # Reply a post
            noti_type = message['commentType']

            sql_query = "select user_id from reviewtydev.post where id = {0} and user_id not in (select user_id from reviewtydev.user_to_unsubscribed_post utup where post_id = {0})"
            sql_query = sql_query.format(comment_data['postId'])
            postAuthor = self.notiService.query(sql_query)
            print(sql_query)
            print(postAuthor)
            try: 
                owner_id = postAuthor[0]['user_id']
            except:
                pass
        else:
            # Reply a comment
            noti_type = message['mentionType']

            sql_query = "select user_id from reviewtydev.post_comment where id = {} and \
                user_id in (select follower_id from reviewtydev.user_to_follower where user_id = {}) and \
                user_id not in (select user_id from reviewtydev.user_to_unsubscribed_post where post_id = {})"
        
            sql_query = sql_query.format(comment_data['parentId'], comment_data['authorId'], comment_data['postId'])
            postAuthor = self.notiService.query(sql_query)
            print(sql_query)
            print(postAuthor)
 
            try: 
                owner_id = postAuthor[0]['user_id']
            except:
                pass
        
        if owner_id >= 0 and owner_id != comment_data['authorId'] and owner_id not in message['mentionIds']:
            receiverIds.append(owner_id)
        
        sql_query = "select distinct(user_id) from reviewtydev.post_comment \
                    where is_deleted = false \
                        and post_id = {} \
                        and parent_id = {} \
                        and user_id not in ({}) \
                        and user_id not in (select follower_id from reviewtydev.user_to_follower where user_id = {}) \
                        and user_id not in (select user_id from reviewtydev.user_to_unsubscribed_post where post_id = {}) \
                "
                
        parentId = comment_data['parentId']
        if parentId == None:
            parentId = 'null'
        
        id_list = [comment_data['authorId']]

        if owner_id > 0:
            id_list.append(owner_id)

        id_list.extend(message['mentionIds'])
        sql_query = sql_query.format(comment_data['postId'], parentId, ', '.join([str(id) for id in id_list]), comment_data['authorId'], comment_data['postId'])
        
        print(sql_query)
        
        postAuthor = self.notiService.query(sql_query)
        receiverIds.extend([row['user_id'] for row in postAuthor])
        
        print(receiverIds)
        
        # Send receiver
        if len(receiverIds) > 0:
            noti_log = {
                'entityId': comment_data['id'],
                'entityType': message['commentEntityType'],
                'type': noti_type,
                'changerIds': [comment_data['authorId']],
                'receiverIds': receiverIds,
            }

            noId = self.notiService.create(data=noti_log)
            user_info = get_users(comment_data['authorId'])[0]
            try:
                avatar = get_avatar(user_info['avatar_id'])
            except:
                avatar = ''

            noti_message = {
                'type': noti_type,
                'account': message['commentAccount'],
                'avatar': avatar,
                'notificationId': noId,
                'postId': comment_data['postId'],
                'postCommentId': comment_data['id']
            }

            device_list = get_device_list_with_notification_config(receiverIds, 'activity')

            self.send_notification(device_list, noti_message, noId=noId)

        # Send mentioner
        if len(message['mentionIds']) > 0:
            sql_query = "select id from reviewtydev.user where id in ({}) and id not in (select user_id from reviewtydev.user_to_unsubscribed_post where post_id = {})"
            sql_query = sql_query.format(', '.join([str(id) for id in message['mentionIds']]), comment_data['postId'])
            user_list = self.notiService.query(sql_query)
            sender_ids = [r['id'] for r in user_list]
            
            noti_log = {
                'entityId': comment_data['id'],
                'entityType': message['commentEntityType'],
                'type': noti_type,
                'changerIds': [comment_data['authorId']],
                'receiverIds': sender_ids,
            }

            noId = self.notiService.create(data=noti_log)

            device_list = get_device_list_with_notification_config(sender_ids, 'activity')
            
            user_info = get_users(comment_data['authorId'])[0]
            try:
                avatar = get_avatar(user_info['avatar_id'])
            except:
                avatar = ''

            noti_message = {
                'type': noti_type,
                'account': message['commentAccount'],
                'avatar': avatar,
                'notificationId': noId,
                'postId': comment_data['postId'],
                'postCommentId': comment_data['id'],
                "no_id": str(noId),
            }
            
            self.send_notification(device_list, noti_message, noId=noId)


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

