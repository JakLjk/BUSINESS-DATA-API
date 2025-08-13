from fastapi import APIRouter
from fastapi.requests import Request
from rq.registry import(
    StartedJobRegistry,
    FinishedJobRegistry,
    FailedJobRegistry,
    DeferredJobRegistry,
    ScheduledJobRegistry
)
from rq import Queue

from business_data_api.api.models import RedisQueueMetadata, RedisQueuesInformation
router = APIRouter()

@router.get(
    "/redis-query-info",
    summary=("Information about state of Redis queries, such as number of jobs awaiting etc"))
async def redis_query_info(
    request:Request
    ):
    metadata_dict = {
        name: RedisQueueMetadata(
            jobs_enqueued=queue.count,
            jobs_started=queue.started_job_registry.count,
            jobs_deferred=queue.deferred_job_registry.count,
            jobs_scheduled=queue.scheduled_job_registry.count,
            jobs_failed=queue.failed_job_registry.count,
            jobs_finished=queue.finished_job_registry.count
        )
        for name, queue in request.app.state.queues.items()
    }
    return RedisQueuesInformation(metadata=metadata_dict)
