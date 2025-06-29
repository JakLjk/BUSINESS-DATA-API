import os 
import sys
import redis
from rq import Worker, Queue
from dotenv import load_dotenv


load_dotenv()

redis_url = os.getenv('REDIS_URL')
conn = redis.from_url(redis_url)

queue_names = [sys.argv[1]] if len(sys.argv) > 1 else ['default']

if __name__ == '__main__':
    queues = [Queue(name, connection=conn) for name in queue_names]
    worker = Worker(queues, connection=conn)
    worker.work()

