from database.database import PostgresqlStorage

class NotificationService():
    def __init__(self) -> None:
        self.postgresql = PostgresqlStorage()

    def query(self, query_sql):
        return self.postgresql.query(query_sql)

    def create(self, data={}):
        app_notification_object = {
            'entity_id': data['entityId'],
            'entity_type': data['entityType'],
            'type': data['type'],
        }
    
        print('Insert new notification', app_notification_object)
        
        noId = self.insert_object("reviewtydev.app_notification_object", app_notification_object)
        
        print("NoID ", noId)

        if noId:
            for changerId in data.get('changerIds',[]):
                print('Insert notification changer', changerId)
                self.insert_object("reviewtydev.app_notification_change", {
                    'user_id': changerId,
                    'notification_object_id': noId,
                })

            receiverIds = data.get('receiverIds', [])
            insert_data = []
            for receiverId in receiverIds:
                # print('Insert notification receiver', receiverId)
                insert_data.append({
                    'user_id': receiverId,
                    'notification_object_id': noId,
                })

            STEP = 10000
            for idx in range(0, len(insert_data), STEP):
                print("Start IDX: ", idx)
                self.insert_bulk_fast("reviewtydev.app_notification", insert_data[idx: idx+STEP])

        return noId
    
    def insert_object(self, db_name, data={}):

        key_list = data.keys()

        insert_sql = '''
            INSERT INTO {}
                ({})
                VALUES
                ({})
                RETURNING id
            '''.format(db_name, ', '.join(key_list), ', '.join(['%s']*len(data)))
                    
        # print(key_list, tuple([data[key] for key in key_list]))

        return self.postgresql.insert_one(insert_sql, tuple([data[key] for key in key_list]))
    
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

    def insert_bulk_fast(self, db_name, data_list=[{}]):

        key_list = data_list[0].keys()

        insert_sql = '''
            INSERT INTO {}
                ({})
                VALUES
            '''.format(db_name, ', '.join(key_list))

        
        tuple_list = [tuple([data[key] for key in key_list]) for data in data_list]

        # print(insert_sql, tuple_list)
        return self.postgresql.insert_bulk_fast(insert_sql, tuple_list)
    
    def update(self, db_name, condition, data):
        
        key_condition = condition.keys()
        clause_condition  = [ key+'=%s' for key in key_condition ]
        
        key_data = data.keys()
        clause_data = [ key+' = %s' for key in key_data ]
        
        update_query = '''
            UPDATE {}
                SET {}
                WHERE {}
            '''.format(db_name, ', '.join(clause_data), ' and '.join(clause_condition))
        
        # print(update_query)

        return self.postgresql.update(update_query, tuple([data[key] for key in key_data]+[condition[key] for key in key_condition]))

    def updateWhere(self, db_name, where_condition, data):
        
        # key_condition = condition.keys()
        # clause_condition  = [ key+'=%s' for key in key_condition ]
        
        key_data = data.keys()
        clause_data = [ key+' = %s' for key in key_data ]
        
        update_query = '''
            UPDATE {}
                SET {}
                WHERE {}
            '''.format(db_name, ', '.join(clause_data), where_condition)
        
        return self.postgresql.update(update_query, tuple([data[key] for key in key_data]))
