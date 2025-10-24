from flask import Flask
from flask_socketio import SocketIO
from logs.update_logger import get_logger
scheduler_logger = get_logger("scheduler")

# Create SocketIO instance
socketio = SocketIO()

def create_app():
    from dotenv import load_dotenv
    import os
    load_dotenv()
    
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    app = Flask(
        __name__,
        static_folder=os.path.join(base_dir, 'static'),
        template_folder=os.path.join(base_dir, 'templates')
    )

    # Load configs
    env = os.getenv("FLASK_ENV", "production")
    if env == "development":
        app.config["DEBUG"] = True
    
    app.config['SECRET_KEY'] = os.getenv('APP_SECRET_KEY')

    # CORS origins from env
    cors_origins = os.getenv("SOCKETIO_CORS_ORIGINS", "*")
    if cors_origins != "*":
        cors_origins = cors_origins.split(",")  # allow comma-separated list

    # Initialize SocketIO with dynamic CORS
    socketio.init_app(app, cors_allowed_origins=cors_origins)
    
    # Register views blueprint
    from .views import views
    app.register_blueprint(views, url_prefix='/')
    
    # Register webhook blueprint
    from .webhook import webhook_bp
    app.register_blueprint(webhook_bp)
    
    # Initialize scheduler
    from .scheduler import init_scheduler
    scheduler_logger.info(f"Initializing scheduler in PID {os.getpid()}")
    init_scheduler(app)
    
    # Register custom Jinja filter
    @app.template_filter("datetimeformat")
    def datetimeformat(value, format='%Y-%m-%d %H:%M'):
        from datetime import datetime
        try:
            return datetime.fromtimestamp(value).strftime(format)
        except Exception:
            return "Invalid date"

    return app