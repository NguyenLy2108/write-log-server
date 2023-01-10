
from database.database import PostgresqlStorage
import datetime
if __name__ == '__main__':
    
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    active_event_query = 'select * from event where start_time'
    pass