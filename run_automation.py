import sys, signal, datetime,os
from zoneinfo import ZoneInfo
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_SUBMITTED, EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from logging_utils import setup_logger
from automation_scripts.check_for_krs_updates import check_for_updates
from config import (
    AUTOMATION_REFRESH_INTERVAL_HOURS, 
    AUTOMATION_NUM_OF_DAYS_TO_CHECK,
    KRS_API_URL,
    LOG_TO_POSTGRE_SQL,
    SOURCE_LOG_SYNC_PSQL_URL
    )



if __name__ == "__main__":
    log = setup_logger(
        logger_name="krsapi_scheduler_log",
        log_to_db=LOG_TO_POSTGRE_SQL,
        log_to_db_url=SOURCE_LOG_SYNC_PSQL_URL
    )
    log.propagate = False
    log.info(f"Initialising scheduler pid={os.getpid()}")
    schd = BlockingScheduler(timezone="Europe/Warsaw")
    def schd_event(event):
        if hasattr(event, "scheduled_run_times"):
            times = event.scheduled_run_times
            most_recent_time = times[-1]
        elif hasattr(event, "scheduled_run_time"):
            most_recent_time = event.scheduled_run_time
        else:
            most_recent_time = "N/a"
        if event.code == EVENT_JOB_SUBMITTED:
            log.info("Job submitted id={id} when={when}".format(
                id=event.job_id,
                when=most_recent_time,
            ))
        elif event.code == EVENT_JOB_EXECUTED:
            log.info("Job executed id={id} when={when} retval={retval}".format(
                id=event.job_id,
                when=most_recent_time,
                retval=getattr(event, "retval", None)
            ))
            job = schd.get_job(event.job_id)
            next_run = job.next_run_time.isoformat() if job and job.next_run_time else "No runs planned"
            log.info("Scheduler next run for job {id}: {run_eta}".format(
                id=event.job_id,
                run_eta=next_run))
        elif event.code == EVENT_JOB_ERROR:
            log.error("Job error id={id} when={when}".format(
                id=event.job_id,
                when=most_recent_time,
                exc_info=True
            ))
            job = schd.get_job(event.job_id)
            next_run = job.next_run_time.isoformat() if job and job.next_run_time else "No runs planned"
            log.info("Scheduler next run for job {id}: {run_eta}".format(
                id=event.job_id,
                run_eta=next_run))
    schd.add_listener(schd_event, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED | EVENT_JOB_SUBMITTED)
    job = schd.add_job(
        check_for_updates,
        IntervalTrigger(hours=AUTOMATION_REFRESH_INTERVAL_HOURS, jitter=15),
        next_run_time=datetime.datetime.now(tz=ZoneInfo("Europe/Warsaw")),
        args=[KRS_API_URL, AUTOMATION_NUM_OF_DAYS_TO_CHECK],
        max_instances=1,
        coalesce=True,
        id="krsapi_update"
    )
    log.info("Scheduler next run: {run_eta}".format(run_eta=job.next_run_time.isoformat()))
    def _graceful(*_):
        schd.shutdown(wait=False)
        sys.exit(0)
    signal.signal(signal.SIGTERM, _graceful)
    signal.signal(signal.SIGINT, _graceful)
    schd.start()
