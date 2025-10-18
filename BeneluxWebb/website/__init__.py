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
    
    # Initialize scheduler only if not in debug mode or in the main process
    if app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        from .update_logger import log_message
        log_message("scheduler", f"Initializing scheduler in PID {os.getpid()}", "info")
        init_scheduler(app)
    else:
        from .update_logger import log_message
        log_message("scheduler", f"Skipping scheduler init in reloader PID {os.getpid()}", "info")

        
    # Register webhook blueprint
    app.register_blueprint(webhook_bp)

    return app
    