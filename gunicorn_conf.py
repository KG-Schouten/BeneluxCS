from BeneluxWebb.website import create_app
from BeneluxWebb.website.scheduler import init_scheduler
from logs.update_logger import get_logger

scheduler_logger = get_logger("scheduler")

def when_ready(server):
    scheduler_logger.info("Gunicorn master is ready - initializing scheduler once")
    app = create_app()
    init_scheduler(app)
