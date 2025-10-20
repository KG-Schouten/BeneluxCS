# app/webhook.py
import hmac
import hashlib
import os
import json
from dotenv import load_dotenv
from flask import Blueprint, request, jsonify, abort, Response
from .scheduler import run_async_job
from .update_logger import log_message
from update import update_matches, update_esea_teams_benelux
from database.db_down_update import gather_teams_benelux_primary


# Load .env variables
load_dotenv()
FACEIT_WEBHOOK_URL = os.getenv("FACEIT_WEBHOOK_URL", '/webhook/faceit')
FACEIT_HEADER = os.getenv("FACEIT_HEADER", '')
FACEIT_HEADER_VALUE = os.getenv("FACEIT_HEADER_VALUE", '')

webhook_bp = Blueprint("webhook", __name__)

def faceit_check_teams(team_ids, event_id) -> list:
    df_teams_benelux = gather_teams_benelux_primary()
    
    try:
        teams = []
        for tid in team_ids:
            if (tid, event_id) in zip(df_teams_benelux['team_id'], df_teams_benelux['event_id']):
                teams.append(tid)
        
        return teams

    except Exception:
        return []

def start_background_job(func, *args, lock_name=None):
    import threading
    wrapper = run_async_job(func)
    threading.Thread(target=wrapper, args=args, daemon=True).start()


@webhook_bp.route(FACEIT_WEBHOOK_URL, methods=["POST"])
def faceit_webhook():
    # Verify security header
    received_header = request.headers.get(FACEIT_HEADER)
    if received_header != FACEIT_HEADER_VALUE:
        log_message("webhook", "Unauthorized request blocked.", "warning")
        return jsonify({"error": "Unauthorized"}), 401
    
    # Get payload
    payload = request.get_json(silent=True)
    if payload is None:
        try:
            payload = json.loads(request.data.decode('utf-8'))
        except Exception as e:
            log_message("webhook", f"Invalid payload: {e}", "error")
            return jsonify({"error": "Invalid JSON"}), 400

    log_message("webhook", f"Request received. Payload: {payload}", "info")
    
    # Trigger placeholder job
    if isinstance(payload, dict):
        # --- Check if any of the teams are Benelux ---
        payload_data = payload.get('payload') or {}
        teams = payload_data.get('teams', [])
        team_ids = [team.get('id') for team in teams if team]
        event_id = payload_data.get('entity', {}).get('id')
        
        if not team_ids:
            log_message("webhook", "No teams found, skipping.", "info")
            return jsonify({"status": "No teams"}), 200

        team_ids = faceit_check_teams(team_ids, event_id)
        
        if not team_ids:
            log_message("webhook", "No Benelux teams found after check, skipping.", "info")
            return jsonify({"status": "No Benelux teams"}), 200
        
        match_id = payload['payload'].get('id')
        
        if payload['event'] in ['match_status_ready', 'match_status_configuring']:
            start_background_job(update_matches, [match_id], [event_id])
        elif payload['event'] == 'match_status_finished':
            start_background_job(update_esea_teams_benelux, team_ids, [event_id])

    return jsonify({"status": "Jobs triggered"}), 200

# ========== Twitch Webhook ==========
# Twitch Headers
TWITCH_MESSAGE_ID = 'twitch-eventsub-message-id'
TWITCH_MESSAGE_TIMESTAMP = 'twitch-eventsub-message-timestamp'
TWITCH_MESSAGE_SIGNATURE = 'twitch-eventsub-message-signature'
TWITCH_MESSAGE_TYPE = 'twitch-eventsub-message-type'

# Twitch message types
MESSAGE_TYPE_VERIFICATION = 'webhook_callback_verification'
MESSAGE_TYPE_NOTIFICATION = 'notification'
MESSAGE_TYPE_REVOCATION = 'revocation'

# Prepend string for HMAC
HMAC_PREFIX = 'sha256='

# Twitch Secret
TWITCH_SECRET = os.getenv("TWITCH_SECRET", '').encode('utf-8')

def verify_twitch_signature(request):
    message_id = request.headers.get(TWITCH_MESSAGE_ID, '')
    timestamp = request.headers.get(TWITCH_MESSAGE_TIMESTAMP, '')
    signature = request.headers.get(TWITCH_MESSAGE_SIGNATURE, '')
    body = request.get_data()
    
    if not all([message_id, timestamp, signature]):
        return False
    
    message = message_id + timestamp + body.decode('utf-8')
    computed_hmac = HMAC_PREFIX + hmac.new(TWITCH_SECRET, message.encode('utf-8'), hashlib.sha256).hexdigest()
    
    return hmac.compare_digest(computed_hmac, signature)

@webhook_bp.route("/webhook/twitch", methods=["POST"])
def twitch_webhook():
    if not verify_twitch_signature(request):
        print("Signature verification failed.")
        abort(403)
    
    print("Signatures match.")
    
    message_type = request.headers.get(TWITCH_MESSAGE_TYPE, '').lower()
    data = request.get_json()
    
    # Handle different message types
    if message_type == MESSAGE_TYPE_VERIFICATION:
        challenge = data.get("challenge")
        print(f"Received verification challenge: {challenge}")
        return Response(challenge, status=200, content_type="text/plain")

    elif message_type == MESSAGE_TYPE_NOTIFICATION:
        subscription_type = data["subscription"]["type"]
        event_data = data["event"]
        print(f"Event type: {subscription_type}")
        print(event_data)

        # Respond quickly so Twitch doesn’t time out
        return "", 204

    elif message_type == MESSAGE_TYPE_REVOCATION:
        print(f"{data['subscription']['type']} notifications revoked!")
        print(f"Reason: {data['subscription']['status']}")
        print(f"Condition: {data['subscription']['condition']}")
        return "", 204

    else:
        print(f"Unknown message type: {message_type}")
        return "", 204
    
    
