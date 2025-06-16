import os 
import sys
import redis
from rq import worker, Queue, Connection

redis_url = os.getenv('REDIS_URL')
queue_names = [sys.argv[1] if len(sys.argv[1]>1) else 'default']

conn = redis.from_url(redis_url)

if __name__ == '__main__':
    with Connection(conn):
        worker(list(map(Queue, queue_names)))
        worker.work()

