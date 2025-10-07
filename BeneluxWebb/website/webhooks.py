from flask import Blueprint, request, jsonify
import hmac
import hashlib
import os
from dotenv import load_dotenv
from data_processing.faceit_api.logging_config import function_logger

webhooks = Blueprint('webhooks', __name__)

load_dotenv()
FACEIT_WEBHOOK_SECRET = os.getenv("FACEIT_WEBHOOK_TOKEN")

@webhooks.route("/webhook/faceit", methods=["POST"])
def faceit_webhook():
    header_secret = request.headers.get("X-Faceit-Secret")
    if header_secret != FACEIT_WEBHOOK_SECRET:
        return jsonify({"error": "unauthorized"}), 401

    data = request.json
    function_logger.info(f"Received webhook: {data}")
    return jsonify({"status": "ok"}), 200
