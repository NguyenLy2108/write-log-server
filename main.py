from workers.post_impression_event_worker import PostImpressionEventWorker

if __name__ == '__main__':     
    worker = PostImpressionEventWorker()   
    worker.listen()
   

