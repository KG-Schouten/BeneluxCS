from BeneluxWebb.website import create_app
from BeneluxWebb.website.scheduler import init_scheduler
from BeneluxWebb.website.update_logger import log_message

def when_ready(server):
    log_message("scheduler", "Gunicorn master is ready - initializing scheduler once", "info")
    app = create_app()
    init_scheduler(app)
