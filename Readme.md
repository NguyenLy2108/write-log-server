# Notification Service
[![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://travis-ci.org/joemccann/dillinger)

## Code structure
```
├── Dockerfile
├── Firebase.Dockerfile
├── Readme.md
├── build-firebase-worker.sh
├── build.sh
├── database  
│   ├── __pycache__
│   └── database.py
├── distributor  (Receive events from queue and distribute to processors)
│   ├── __pycache__
│   └── distribute_events.py
├── env
│   ├── bin
│   ├── include
│   ├── lib
│   └── pyvenv.cfg
├── events      (Implemented processors to process events) 
│   ├── AwardPointEvent.py
│   ├── CreatePostEvent.py
│   ├── EventFactory.py
│   ├── EventNotificationAdmin.py
│   ├── EventNotificationAdminResend.py
│   ├── FollowUserEvent.py
│   ├── ReactPostEvent.py
│   ├── ReplyPostEvent.py
│   ├── SaleEvents.py
│   ├── UserRequestEvent.py
│   └── __pycache__
├── listeners       (Listeners collection to make message queues)
│   ├── KafkaListener.py
│   ├── ListenerAbstract.py
│   ├── RabbitMQListener.py
│   └── __pycache__
├── main.py
├── outputor    
│   ├── EventSource.py
│   ├── FirebasePusher.py
│   ├── Websocket.py
│   └── __pycache__
├── requirements.txt
├── reviewty-test-firebase-adminsdk-x8m04-eecfc9bbda.json
├── scheduler
│   └── SaleEventTimer.py
├── services
│   ├── NotificationServer.py
│   └── __pycache__
├── utils
│   ├── __pycache__
│   └── common.py
└── workers     (A other workers for sending notification) 
    ├── FirebaseWorker
    ├── PostEventWorker.py
    └── __pycache__
```

## Making new event or new notification flow
#### 1.  In Graphql API service, make news event by sending data to the message queue
```
const message = {
  type: 'EVENT_NOTIFICATION_ADMIN',
  data: your_data,
  time: new Date(),
};

this.rabbitMQService.produce(
  process.env.RABBIT_TOPIC,
  JSON.stringify(message),
);
```
#### 2. Implement a worker to receive  data from the queue and process them at https://gitlab.com/review-ty/events-processor/-/tree/master
Note: 'name' field of class equal with value of type in the message when pushing data  
```
class EventNotificationAdmin():
    name = 'EVENT_NOTIFICATION_ADMIN'
```

#### 3. Insert notification information to the database
#### 4. Check on/off notification config of users
#### 5. Send notifications by batch to the firebase pusher worker with max 500 devices per a sending time 
#### 6. Firebase worker receives data from queue and push notification to the device  
#### 7. Add new event object to EventFactory.py

## Notification Config Type
```
    activities
    events
    news/upgrade
    messages
    points
```

# Design notification schemas
## App Notification
Store notification information of received users and notification state when sending the notification successfully or not
```
id
created_at
is_viewed
user_id
notification_object_id
is_sent
```

## App Notification Object
Store common information of notification at a sending time. A notification could be sent to multiple receivers.  
```
id
created_at
type
entity_id
entity_type 
```

entity_type:
```
export enum NotificationEntityType {
  POST = 1,
  POST_COMMENT = 2,
  EVENT = 3,
  CAST = 4,
  REVIEW = 5,
  LIVE_STREAM = 6,
  EVENT_ADMIN = 7,
  NEWS_UPGRADE = 8,
  FOLLOW_USER = 9,
  AWARD_POINT = 10,
  USER_REQUEST = 11,
}
```

## App Notification Change
Store information of sender
```
id
created_at
is_viewed
user_id
notification_object_id
```

# Running and Deployments

## Chạy module xử lý và tổng hợp thông tin notification
python main.py

## Chạy worker nhận thông tin notificcation và gửi lên firebase
python -m workers.FirebaseWorker.worker

## Dockerfile

### Build event processor
sudo docker build -t reviewty/events-processor:4.2.0 .

### Build firebase worker
sudo docker build -t reviewty/firebase-worker:4.2.0 .# write-log-server
