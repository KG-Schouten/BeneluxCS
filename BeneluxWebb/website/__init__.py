from flask import Flask
from datetime import datetime
import os
from .scheduler import init_scheduler
from .webhook import webhook_bp

def create_app():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    app = Flask(
        __name__,
        static_folder=os.path.join(base_dir, 'static'),
        template_folder=os.path.join(base_dir, 'templates')
    )

    app.config['SECRET_KEY'] = 'pqwerutyiryrwqer'

    # Register custom Jinja filter
    @app.template_filter("datetimeformat")
    def datetimeformat(value, format='%Y-%m-%d %H:%M'):
        try:
            return datetime.fromtimestamp(value).strftime(format)
        except Exception:
            return "Invalid date"

    from .views import views
    app.register_blueprint(views, url_prefix='/')
    
    import pprint
    pprint.pprint(dict(os.environ))

    # Initialize scheduler only if not in debug mode or in the main process
    from .update_logger import log_message
    if os.environ.get("GUNICORN_CMD") is None:
        # Running locally, Flask dev server
        log_message("scheduler", f"Initializing scheduler in PID {os.getpid()} {os.environ.get('GUNICORN_CMD')}", "info")
        init_scheduler(app)
    else:
        # Running under Gunicorn, skip scheduler here
        log_message("scheduler", f"Gunicorn worker PID {os.getpid()} {os.environ.get('GUNICORN_CMD')} — skipping scheduler", "info")
        
    # Register webhook blueprint
    app.register_blueprint(webhook_bp)

    return app
    