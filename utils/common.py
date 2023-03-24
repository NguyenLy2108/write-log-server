

from services.NotificationServer import NotificationService


notiService = NotificationService()


'''
    notification_config_type:
        activity
        events
        news
        message
        points
'''
def get_device_list_with_notification_config(user_id_list = [], notification_config_type = ''):
    sql_query = "select d.token, d.user_id from reviewtydev.device d left join reviewtydev.user_notification_config unc on unc.user_id = d.user_id where d.user_id in ({}) \
             and (unc.{} is null or unc.{} = true)\
        ".format(
            ', '.join([str(id) for id in user_id_list]),
            notification_config_type,
            notification_config_type
        )

    return notiService.query(sql_query)

def get_users(user_id = ''):
        sql_query = f'select u.account_name , u.avatar_id from reviewtydev.user u where u.id={user_id}'
        return notiService.query(sql_query)

def get_avatar(image_id):
    sql_query = f'select url from reviewtydev.image where id={image_id}'
    return notiService.query(sql_query)[0]['url']

