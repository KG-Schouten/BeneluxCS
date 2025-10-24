import eventlet
eventlet.monkey_patch()

from BeneluxWebb.website import create_app, socketio
import os

app = create_app()

if __name__ == "__main__" and os.getenv("FLASK_ENV") == "development":
    socketio.run(app, host="0.0.0.0", port=5000, debug=True)
