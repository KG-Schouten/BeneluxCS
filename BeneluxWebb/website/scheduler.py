# app/scheduler.py
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import redis
from contextlib import contextmanager
import os
from dotenv import load_dotenv
import time

import asyncio
from update import (
    update_ongoing_matches,
    update_upcoming_matches,
    update_esea_teams_benelux,
    update_new_matches_hub,
    update_new_matches_esea,
    update_leaderboard,
    update_elo_leaderboard,
    update_league_teams,
    update_team_avatars,
    update_local_team_avatars,
    update_hub_events,
    update_esea_seasons_events,
    update_twitch_streams_benelux,
    update_live_streams,
    update_eventsub_subscriptions
)

from logs.update_logger import get_logger

scheduler_logger = get_logger("scheduler")

load_dotenv()

# ------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

# ------------------
# Redis lock
# ------------------
redis_client = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

@contextmanager
def redis_lock(lock_name, timeout=60, wait=True):
    if REDIS_HOST == "localhost":
        yield
        return
    
    got_lock = False
    start = time.time()
    while not got_lock:
        got_lock = redis_client.set(lock_name, "1", nx=True, ex=timeout)
        if got_lock:
            try:
                yield
            finally:
                redis_client.delete(lock_name)
            break
        elif not wait:
            scheduler_logger.info( f"[LOCK SKIPPED] {lock_name} is already locked")
            break
        else:
            time.sleep(0.5)
            # optional: prevent infinite wait
            if time.time() - start > timeout:
                scheduler_logger.warning( f"[LOCK TIMEOUT] {lock_name} waited too long")
                break

# ------------------
# Wrapper for async jobs with optional lock
# ------------------
GLOBAL_SCHEDULER_LOCK = "scheduler_global_lock"
def run_async_job(job_func, *args, **kwargs):
    def wrapper():
        job_name = job_func.__name__
        scheduler_logger.info(f"[JOB START] {job_name}")
        try:
            job_func(*args, **kwargs)
            scheduler_logger.info(f"[JOB FINISH] {job_name}")
        except Exception as e:
            scheduler_logger.error(f"[JOB ERROR] {job_name}: {e}")

    # Run as a green thread
    import eventlet
    eventlet.spawn(wrapper)

# ------------------
# Scheduler initialization
# ------------------
def init_scheduler(app):    
    scheduler = BackgroundScheduler(timezone="UTC")

    try:
        # --- Every minute ---
        scheduler.add_job(
            run_async_job(update_ongoing_matches),
            CronTrigger(minute="*")
        )
        
        # --- Every 5 minutes ---
        scheduler.add_job(
            run_async_job(update_live_streams),
            CronTrigger(minute="*/5")
        )
        scheduler.add_job(
            run_async_job(update_twitch_streams_benelux),
            CronTrigger(minute="*/5")
        )
        scheduler.add_job(
            run_async_job(update_eventsub_subscriptions),
            CronTrigger(minute="*/5")
        )
        
        # --- Hourly ---
        scheduler.add_job(
            run_async_job(update_leaderboard),
            CronTrigger(minute=0)
        )
        scheduler.add_job(
            run_async_job(update_elo_leaderboard),
            CronTrigger(minute=0)
        )
        scheduler.add_job(
            run_async_job(update_new_matches_hub),
            CronTrigger(minute=0)
        )
        scheduler.add_job(
            run_async_job(update_new_matches_esea),
            CronTrigger(minute=0)
        )
        scheduler.add_job(
            run_async_job(update_upcoming_matches),
            CronTrigger(minute=0)
        )
        scheduler.add_job(
            run_async_job(update_esea_teams_benelux),
            CronTrigger(minute=0)
        )

        # --- Daily (00:00 UTC) ---
        scheduler.add_job(
            run_async_job(update_league_teams),
            CronTrigger(hour=0, minute=0)
        )
        scheduler.add_job(
            run_async_job(update_team_avatars),
            CronTrigger(hour=0, minute=0)
        )
        scheduler.add_job(
            run_async_job(update_local_team_avatars),
            CronTrigger(hour=0, minute=0)
        )

        # --- Weekly (Sunday 00:00 UTC) ---
        scheduler.add_job(
            run_async_job(update_hub_events),
            CronTrigger(day_of_week="sun", hour=0, minute=0)
        )
        scheduler.add_job(
            run_async_job(update_esea_seasons_events),
            CronTrigger(day_of_week="sun", hour=0, minute=0)
        )
    except Exception as e:
        scheduler_logger.error(f"[INIT] Error adding jobs to scheduler: {e}")
    
    scheduler.start()
    scheduler_logger.info("--- APScheduler placeholder started ---")
    
    import atexit
    atexit.register(lambda: scheduler.shutdown(wait=False))
    
    app.scheduler = scheduler