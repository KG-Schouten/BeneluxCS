# app/webhook.py
import os
from dotenv import load_dotenv
from flask import Blueprint, request, jsonify
from .scheduler import run_async_job, log_placeholder
from .update_logger import log_message

# Load .env variables
load_dotenv()
FACEIT_WEBHOOK_URL = os.getenv("FACEIT_WEBHOOK_URL", '/webhook/faceit')
FACEIT_HEADER = os.getenv("FACEIT_HEADER", '')
FACEIT_HEADER_VALUE = os.getenv("FACEIT_HEADER_VALUE", '')

webhook_bp = Blueprint("webhook", __name__)

@webhook_bp.route(FACEIT_WEBHOOK_URL, methods=["POST"])
def faceit_webhook():
    # Verify security header
    received_header = request.headers.get(FACEIT_HEADER)
    if received_header != FACEIT_HEADER_VALUE:
        log_message("webhook", "[WEBHOOK] Unauthorized request blocked.", "warning")
        return jsonify({"error": "Unauthorized"}), 401
    
    # Get payload
    try:
        payload = request.get_json(force=True)  # Parse JSON
    except Exception:
        payload = request.data.decode('utf-8')  # Fallback to raw data

    log_message("webhook", f"[WEBHOOK] Request received. Payload: {payload}", "info")
    
    # Trigger placeholder job
    wrapper = run_async_job(log_placeholder("Webhook job"), lock_name="func_placeholder")
    
    # Run in background so webhook responds quickly
    import threading
    threading.Thread(target=wrapper, daemon=True).start()

    return jsonify({"status": "Jobs triggered"}), 200
