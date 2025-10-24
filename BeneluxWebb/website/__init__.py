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
    
    scheduler_logger.debug("Creating Flask app")
    
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    scheduler_logger.debug(f"Base directory set to {base_dir}")
    
    app = Flask(
        __name__,
        static_folder=os.path.join(base_dir, 'static'),
        template_folder=os.path.join(base_dir, 'templates')
    )

    # Load configs
    env = os.getenv("FLASK_ENV", "production")
    if env == "development":
        app.config["DEBUG"] = True
        scheduler_logger.debug("App running in development mode")
    else:
        app.config["DEBUG"] = False
        scheduler_logger.debug("App running in production mode")
    
    app.config['SECRET_KEY'] = os.getenv('APP_SECRET_KEY')

    # CORS origins from env
    cors_origins = os.getenv("SOCKETIO_CORS_ORIGINS", "*")
    if cors_origins != "*":
        cors_origins = cors_origins.split(",")  # allow comma-separated list
    
    scheduler_logger.debug(f"SocketIO CORS origins set to: {cors_origins}")

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