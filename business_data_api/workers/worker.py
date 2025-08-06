import redis
from rq import Worker, Queue
from typing import Literal

from config import REDIS_URL

redis_url = REDIS_URL
conn = redis.from_url(redis_url)


def run_worker(queue_name:Literal["KRSAPI", "KRSDF"]):
    queue = Queue(queue_name, connection=conn)
    worker = Worker(queue, connection=conn)
    worker.work()

