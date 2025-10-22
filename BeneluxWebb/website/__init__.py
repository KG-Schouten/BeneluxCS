from flask import Flask
from datetime import datetime
import os
from .scheduler import init_scheduler
from .webhook import webhook_bp
from logs.update_logger import get_logger
from dotenv import load_dotenv
load_dotenv()
scheduler_logger = get_logger("scheduler")

def create_app():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    app = Flask(
        __name__,
        static_folder=os.path.join(base_dir, 'static'),
        template_folder=os.path.join(base_dir, 'templates')
    )

    app.config['SECRET_KEY'] = os.getenv('APP_SECRET_KEY')

    # Register custom Jinja filter
    @app.template_filter("datetimeformat")
    def datetimeformat(value, format='%Y-%m-%d %H:%M'):
        try:
            return datetime.fromtimestamp(value).strftime(format)
        except Exception:
            return "Invalid date"

    from .views import views
    app.register_blueprint(views, url_prefix='/')
    
    # Initialize scheduler only if not in debug mode or in the main process
    if os.environ.get("FLASK_ENV", "") != "development": 
        if app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
            scheduler_logger.info(f"Initializing scheduler in PID {os.getpid()}")
            init_scheduler(app)
        else:
            scheduler_logger.info(f"Skipping scheduler init in reloader PID {os.getpid()}")
    else:
        scheduler_logger.info(f"Not initializing scheduler in dev mode {os.getpid()}")

    # Register webhook blueprint
    app.register_blueprint(webhook_bp)

    return app
    