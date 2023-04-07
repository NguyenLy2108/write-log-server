import json
from enum import Enum
from database.database import PostgresqlStorage
import config

cfg = config.Settings()

class Mode(Enum):
    CLICK = 1
    SINGLE = 2
    MULTIPLE = 3

class DeviceType(Enum):
    WEB = 1
    MOBILE = 2

class EntityType(Enum):
    POST = 1
    REVIEW = 2

class PostImpresstionService():
    key = 'system.config.Powerful-Like'
    duration = 1800
    
    def __init__(self) -> None:
        self.postgresql = PostgresqlStorage()

    def query(self, query_sql):
        return self.postgresql.query(query_sql)
    
    def get_power_full_like(self, conn):                     
        cache_value = conn.get(self.key)
        if cache_value:
            return int(cache_value)
        
        query_sql = '''
            SELECT value FROM reviewtydev.system_config WHERE name = 'Powerful-Like'
        '''   
        value = self.query(query_sql)[0]['value']
        if value and self.duration:
            conn.setex(self.key, self.duration, str(value))
        else:
            conn.set(self.key, str(value))

        return int(value)
           

    def create(self, pre_data, conn):
        post_impression_objects = []        
        
        power = self.get_power_full_like(conn)   

        impression_log = {}    

        if pre_data['name'] in ['post_view_list', 'review_view_list']:
            post_ids = json.loads(pre_data['data'])['content_list']
            post_ids = post_ids.split(',')
            
            for r in post_ids: 
                try:
                    r = r.split('_')
                    post_id = int(r[-1])
                    if(len(r) > 1):
                        entity_type = EntityType.REVIEW.value if(r[0]=='RW') else EntityType.POST.value                    
                    else:
                        entity_type = EntityType.POST.value if (pre_data['name'] == 'post_view_list') else EntityType.REVIEW.value

                    post_impression_objects.append({
                        "post_id": post_id,
                        "user_id": int(pre_data['user_id']),
                        "entity_type": entity_type,
                        "device_type": DeviceType.MOBILE.value,
                        "mode": Mode.MULTIPLE.value if len(post_ids) > 1 else Mode.SINGLE.value,
                        # "device_token": r['device_token'] if 'device_token' in r else None,
                        "power": power
                    })
                    impression_log = {
                        'query': pre_data['query'],
                        'device_token': pre_data['device_token'] if 'device_token' in pre_data else None,
                        'timestamp': pre_data['timestamp'],
                        'created_at': pre_data['created_at'],
                        'input':{
                            'post_id': str(post_id),
                            'post_type': "post" if ( entity_type == 1) else "review",
                        },
                        'user_id': str(pre_data['user_id']),
                        'event': pre_data['event'],
                        'ip' : pre_data['ip'] if "ip" in pre_data else None
                    }

                    # publish data into channel name redis_pub_topic                   
                    conn.publish(cfg.redis_pub_topic, str(impression_log))

                except Exception as error:
                    print("######Message error: ", error, pre_data)   

        if pre_data['name'] in ['select_post_item', 'select_review_item']:
            try:
                post_impression_objects.append({
                    "post_id": int(json.loads(pre_data['data'])['content_id']),
                    "user_id": int(pre_data['user_id']),
                    "entity_type": EntityType.POST.value if (pre_data['name'] == 'select_post_item') else EntityType.REVIEW.value,
                    "device_type": DeviceType.MOBILE.value,
                    "mode": Mode.CLICK.value,
                    # "device_token": r['device_token'] if 'device_token' in r else None,
                    "power": power
                })                

            except Exception as error:
                print("######Message error: ", error, pre_data)               

        if (len(post_impression_objects) > 0):    
            # print('-----Insert new post impression-----: ', post_impression_objects)            
            self.insert_bulk("reviewtydev.server_log_view_post", post_impression_objects)             
    
    def insert_bulk(self, db_name, data_list=[{}]):

        key_list = data_list[0].keys()

        insert_sql = '''
            INSERT INTO {}
                ({})
                VALUES
                ({})
            '''.format(db_name, ', '.join(key_list), ', '.join(['%s']*len(data_list[0])))

        
        tuple_list = [tuple([data[key] for key in key_list]) for data in data_list]
        # print(insert_sql, tuple_list)
        return self.postgresql.insert_bulk(insert_sql, tuple(tuple_list))