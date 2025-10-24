import eventlet
import asyncio
from redis.lock import Lock
import redis
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from logs.update_logger import get_logger
scheduler_logger = get_logger("scheduler")

# Semaphore to ensure only one task runs at a time (per process)
job_lock = eventlet.semaphore.Semaphore(1)

# Redis-based distributed lock
USE_REDIS_LOCK = False
try:
    r = redis.Redis.from_url("redis://localhost:6379/0")
    USE_REDIS_LOCK = True
except ImportError:
    scheduler_logger.warning("Redis not installed, distributed lock disabled.")

def run_async(func, *args, **kwargs):
    """
    Wrap an async function to run in an Eventlet green thread.
    """
    def wrapper():
        scheduler_logger.info(f"Attempting to acquire lock for {func.__name__}")

        # Acquire local process lock
        with job_lock:
            if USE_REDIS_LOCK:
                lock_name = f"flask_task_lock:{func.__name__}"
                with Lock(r, lock_name, timeout=3600):
                    scheduler_logger.info(f"Acquired Redis lock for {func.__name__}")
                    _run(func, *args, **kwargs)
            else:
                _run(func, *args, **kwargs)

    return wrapper

def _run(func, *args, **kwargs):
    """Helper to run the async function in a new asyncio loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        scheduler_logger.info(f"Starting async task {func.__name__}")
        loop.run_until_complete(func(*args, **kwargs))
        scheduler_logger.info(f"Finished async task {func.__name__}")
    except Exception as e:
        scheduler_logger.error(f"Error running task {func.__name__}: {e}")
    finally:
        loop.close()
        
# ------------------
# Scheduler initialization
# ------------------
def init_scheduler(app):
    """
    Initialize APScheduler and schedule your jobs.
    """
    import update

    scheduler = BackgroundScheduler()
    scheduler.start()

    try:
        # --- Every minute ---
        scheduler.add_job(
            run_async(update.update_ongoing_matches),
            trigger=CronTrigger(minute="*"),
        )
        
        # --- Every 5 minutes ---
        scheduler.add_job(
            run_async(update.update_live_streams),
            trigger=CronTrigger(minute="*/5")
        )
        scheduler.add_job(
            run_async(update.update_twitch_streams_benelux),
            trigger=CronTrigger(minute="*/5")
        )
        scheduler.add_job(
            run_async(update.update_eventsub_subscriptions),
            trigger=CronTrigger(minute="*/5")
        )
        
        # --- Hourly ---
        scheduler.add_job(
            run_async(update.update_leaderboard),
            trigger=CronTrigger(minute=0)
        )
        scheduler.add_job(
            run_async(update.update_elo_leaderboard),
            trigger=CronTrigger(minute=0)
        )
        scheduler.add_job(
            run_async(update.update_new_matches_hub),
            trigger=CronTrigger(minute=0)
        )
        scheduler.add_job(
            run_async(update.update_new_matches_esea),
            trigger=CronTrigger(minute=0)
        )
        scheduler.add_job(
            run_async(update.update_upcoming_matches),
            trigger=CronTrigger(minute=0)
        )
        scheduler.add_job(
            run_async(update.update_esea_teams_benelux),
            trigger=CronTrigger(minute=0)
        )

        # --- Daily (00:00 UTC) ---
        scheduler.add_job(
            run_async(update.update_league_teams),
            trigger=CronTrigger(hour=0, minute=0)
        )
        scheduler.add_job(
            run_async(update.update_team_avatars),
            trigger=CronTrigger(hour=0, minute=0)
        )
        scheduler.add_job(
            run_async(update.update_local_team_avatars),
            trigger=CronTrigger(hour=0, minute=0)
        )

        # --- Weekly (Sunday 00:00 UTC) ---
        scheduler.add_job(
            run_async(update.update_hub_events),
            trigger=CronTrigger(day_of_week="sun", hour=0, minute=0)
        )
        scheduler.add_job(
            run_async(update.update_esea_seasons_events),
            trigger=CronTrigger(day_of_week="sun", hour=0, minute=0)
        )
        
    except Exception as e:
        scheduler_logger.error(f"[INIT] Error adding jobs: {e}", exc_info=True)
    
    # Store scheduler on app for optional later access
    app.scheduler = scheduler
    scheduler_logger.info("Scheduler initialized and jobs scheduled.")
    return scheduler