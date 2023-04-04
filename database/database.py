import os

from pydantic import BaseModel
import psycopg2
import psycopg2.extras
import config

class PostgresqlStorage():
    def __init__(self) -> None:
        super().__init__()
        if len(os.environ) > 0:
            self.config = config.Settings(**os.environ)
        else:
            self.config = config.Settings()
        self.connect()

    def connect(self):
        self.conn = psycopg2.connect(
            host=self.config.postgre_host,
            port=self.config.postgre_port,
            dbname=self.config.postgre_database,
            user=self.config.postgre_user,
            password=self.config.postgre_password,
            target_session_attrs="read-write"
        )

    def query(self, sql=''):
        if self.conn.closed == 0:
            self.connect()

        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(sql)
        ans =cur.fetchall()
        dict_result = []
        for row in ans:
            dict_result.append(dict(row))
        return dict_result

    def insert_one(self, insert_sql, data_tuple):
        try:
            cursor = self.conn.cursor()
            cursor.execute(insert_sql, data_tuple)
            self.conn.commit()
            id_of_new_row = cursor.fetchone()[0]
            return id_of_new_row
        except Exception as error:
            print(error)
            self.conn.rollback()
        return None    
    
    def insert_bulk(self, insert_sql, data_tuple):
        try:
            cursor = self.conn.cursor()
            cursor.executemany(insert_sql, data_tuple)
            self.conn.commit()
        except Exception as error:
            print(error)
            self.conn.rollback()
        return None

    def insert_bulk_fast(self, insert_sql, data_list):
        try:
            cursor = self.conn.cursor()
            # print(data_list[0])
            # args_str = b','.join(cursor.mogrify('(' + ', '.join(["%s"]*len(data_list[0])) + ')', x) for x in data_list) 
            args_str = b','.join(cursor.mogrify("(%s,%s)", x) for x in data_list) 
            
            # cursor.execute(b'INSERT INTO reviewtydev.app_notification (user_id, notification_object_id) VALUES ' + args_str)
            cursor.execute(bytes(insert_sql, encoding = 'UTF-8') + args_str)
            self.conn.commit()
        except Exception as error:
            print(error)
            self.conn.rollback()
        return None    

    def update(self, update_query, data_tuple):
        try:
            cursor = self.conn.cursor()
            cursor.execute(update_query, data_tuple)
            self.conn.commit()
            cursor.close()
        except Exception as error:
            print(error)
            self.conn.rollback()
