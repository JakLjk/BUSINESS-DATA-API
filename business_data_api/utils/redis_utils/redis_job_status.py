from rq.Job import Job
from redis import Redis

def job_is_running(job_id:str, redis_conn:Redis)
    try:
        job = job.fetch(job_id, conection=redis_conn)
        return job.status in ["queued", "started"]