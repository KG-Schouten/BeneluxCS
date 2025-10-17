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
    
    # Initialize the scheduler (only once, if not in debug reload)
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not app.debug:
        init_scheduler(app)
        
    # Register webhook blueprint
    app.register_blueprint(webhook_bp)

    return app
    