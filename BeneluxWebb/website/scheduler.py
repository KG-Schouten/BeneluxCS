# app/scheduler.py
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import redis
from contextlib import contextmanager
import os
from dotenv import load_dotenv

import asyncio
# from update import (
#     update_matches,
#     update_esea_teams_benelux,
#     update_new_matches_hub,
#     update_new_matches_esea,
#     update_leaderboard,
#     update_elo_leaderboard,
#     update_league_teams,
#     update_team_avatars,
#     update_local_team_avatars,
#     update_hub_events,
#     update_esea_seasons_events
# )

from .update_logger import log_message

load_dotenv()

# ------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

# ------------------
# Redis lock
# ------------------
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

@contextmanager
def redis_lock(lock_name, timeout=60):
    if REDIS_HOST == "localhost":
        yield
        return
    
    got_lock = redis_client.set(lock_name, "1", nx=True, ex=timeout)
    if got_lock:
        try:
            yield
        finally:
            redis_client.delete(lock_name)
    else:
        log_message("scheduler", f"[LOCK SKIPPED] {lock_name} is already locked", "info")

# ------------------
# Placeholder async job
# ------------------
def log_placeholder(job_name):
    async def job():
        log_message("scheduler", f"[PLACEHOLDER] Job '{job_name}' started", "info")
    return job

# ------------------
# Wrapper for async jobs with optional lock
# ------------------
def run_async_job(job_func, lock_name=None, lock_timeout=300):
    def wrapper():
        async def runner():
            try:
                await job_func()
            except Exception as e:
                log_message("scheduler", f"[ERROR] {job_func.__name__}: {e}", "error")

        if lock_name:
            with redis_lock(lock_name, timeout=lock_timeout):
                asyncio.run(runner())
        else:
            asyncio.run(runner())
    return wrapper


# ------------------
# Scheduler initialization
# ------------------
async def func_placeholder():
    log_message("scheduler", "[PLACEHOLDER] This is a placeholder function.", "info")


def init_scheduler(app):
    scheduler = BackgroundScheduler(timezone="UTC")

    # --- Every 5 min ---
    scheduler.add_job(
        run_async_job(log_placeholder("update_matches"), lock_name="func_placeholder"),
        CronTrigger(minute="*/5")
    )
    # scheduler.add_job(
    #     run_async_job(log_placeholder("update_esea_teams_benelux"), lock_name="func_placeholder"),
    #     CronTrigger(minute="*/5")
    # )

    # --- Hourly ---
    scheduler.add_job(
        run_async_job(log_placeholder("update_leaderboard"), lock_name="func_placeholder"),
        CronTrigger(minute=0)
    )
    # scheduler.add_job(
    #     run_async_job(log_placeholder("update_elo_leaderboard"), lock_name="func_placeholder"),
    #     CronTrigger(minute=0)
    # )
    # scheduler.add_job(
    #     run_async_job(log_placeholder("update_new_matches_hub"), lock_name="func_placeholder"),
    #     CronTrigger(minute=0)
    # )
    # scheduler.add_job(
    #     run_async_job(log_placeholder("update_new_matches_esea"), lock_name="func_placeholder"),
    #     CronTrigger(minute=0)
    # )

    # # --- Daily ---
    # scheduler.add_job(
    #     run_async_job(log_placeholder("update_league_teams"), lock_name="func_placeholder"),
    #     CronTrigger(hour=0, minute=0)
    # )
    # scheduler.add_job(
    #     run_async_job(log_placeholder("update_team_avatars"), lock_name="func_placeholder"),
    #     CronTrigger(hour=0, minute=0)
    # )
    # scheduler.add_job(
    #     run_async_job(log_placeholder("update_local_team_avatars"), lock_name="func_placeholder"),
    #     CronTrigger(hour=0, minute=0)
    # )

    # # --- Weekly ---
    # scheduler.add_job(
    #     run_async_job(log_placeholder("update_hub_events"), lock_name="func_placeholder"),
    #     CronTrigger(day_of_week="sun", hour=0, minute=0)
    # )
    # scheduler.add_job(
    #     run_async_job(log_placeholder("update_esea_seasons_events"), lock_name="func_placeholder"),
    #     CronTrigger(day_of_week="sun", hour=0, minute=0)
    # )

    scheduler.start()
    log_message("scheduler", "--- APScheduler placeholder started ---", "info")
    
    import atexit
    atexit.register(lambda: scheduler.shutdown(wait=False))
    
    app.scheduler = scheduler


# def init_scheduler(app):
#     scheduler = BackgroundScheduler(timezone="UTC")

#     # --- Every 5 min ---
#     scheduler.add_job(
#         run_async_job(update_matches, lock_name="update_matches_lock"),
#         CronTrigger(minute="*/5")
#     )
#     scheduler.add_job(
#         run_async_job(update_esea_teams_benelux, lock_name="update_esea_teams_lock"),
#         CronTrigger(minute="*/5")
#     )

#     # --- Hourly ---
#     scheduler.add_job(
#         run_async_job(update_leaderboard, lock_name="update_leaderboard_lock"),
#         CronTrigger(minute=0)
#     )
#     scheduler.add_job(
#         run_async_job(update_elo_leaderboard, lock_name="update_elo_lock"),
#         CronTrigger(minute=0)
#     )
#     scheduler.add_job(
#         run_async_job(update_new_matches_hub, lock_name="update_hub_lock"),
#         CronTrigger(minute=0)
#     )
#     scheduler.add_job(
#         run_async_job(update_new_matches_esea, lock_name="update_esea_lock"),
#         CronTrigger(minute=0)
#     )

#     # --- Daily (00:00 UTC) ---
#     scheduler.add_job(
#         run_async_job(update_league_teams, lock_name="update_league_teams_lock"),
#         CronTrigger(hour=0, minute=0)
#     )
#     scheduler.add_job(
#         run_async_job(update_team_avatars, lock_name="update_team_avatars_lock"),
#         CronTrigger(hour=0, minute=0)
#     )
#     scheduler.add_job(
#         run_async_job(update_local_team_avatars, lock_name="update_local_team_avatars_lock"),
#         CronTrigger(hour=0, minute=0)
#     )

#     # --- Weekly (Sunday 00:00 UTC) ---
#     scheduler.add_job(
#         run_async_job(update_hub_events, lock_name="update_hub_events_lock"),
#         CronTrigger(day_of_week="sun", hour=0, minute=0)
#     )
#     scheduler.add_job(
#         run_async_job(update_esea_seasons_events, lock_name="update_esea_seasons_events_lock"),
#         CronTrigger(day_of_week="sun", hour=0, minute=0)
#     )

#     scheduler.start()
#     scheduler_logger.info("--- APScheduler placeholder started ---")
    
#     import atexit
#     atexit.register(lambda: scheduler.shutdown(wait=False))
    
#     app.scheduler = scheduler
