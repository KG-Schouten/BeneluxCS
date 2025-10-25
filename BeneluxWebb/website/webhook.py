# app/webhook.py
import hmac
import hashlib
import os
import json
import eventlet

from .scheduler import run_async
from dotenv import load_dotenv
from flask import Blueprint, request, jsonify, abort, Response
from logs.update_logger import get_logger
from update import update_matches, update_esea_teams_benelux, update_streamers
from database.db_down_update import gather_teams_benelux_primary

webhook_logger = get_logger("webhook")

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

@webhook_bp.route(FACEIT_WEBHOOK_URL, methods=["POST"])
def faceit_webhook():
    # Verify security header
    received_header = request.headers.get(FACEIT_HEADER)
    if received_header != FACEIT_HEADER_VALUE:
        webhook_logger.warning("Unauthorized request blocked.")
        return jsonify({"error": "Unauthorized"}), 401
    
    # Get payload
    payload = request.get_json(silent=True)
    if payload is None:
        try:
            payload = json.loads(request.data.decode('utf-8'))
        except Exception as e:
            webhook_logger.error(f"Invalid JSON payload received: {e}", exc_info=True)
            return jsonify({"error": "Invalid JSON"}), 400
    
    # Trigger placeholder job
    if isinstance(payload, dict):
        if payload.get('payload', {}).get('region') != 'EU':
            webhook_logger.debug("Non-EU region payload received, skipping.")
            return jsonify({"status": "Non-EU region"}), 200
        
        # --- Check if any of the teams are Benelux ---
        payload_data = payload.get('payload') or {}
        teams = payload_data.get('teams', [])
        team_ids = [team.get('id') for team in teams if team]
        event_id = payload_data.get('entity', {}).get('id')
        
        if not team_ids:
            webhook_logger.debug("No teams found in payload, skipping.")
            return jsonify({"status": "No teams"}), 200

        team_ids = faceit_check_teams(team_ids, event_id)
        
        if not team_ids:
            webhook_logger.debug("No Benelux teams found after check, skipping. ")
            return jsonify({"status": "No Benelux teams"}), 200
        
        webhook_logger.info(f"Correct payload received: {payload}")
        
        match_id = payload['payload'].get('id')
        
        if payload['event'] in ['match_status_ready', 'match_status_configuring']:
            eventlet.spawn(run_async(update_matches, [match_id], [event_id]))
            webhook_logger.info(f"Triggering match update for match ID: {match_id}")
        elif payload['event'] == 'match_status_finished':
            eventlet.spawn(run_async(update_esea_teams_benelux, team_ids, [event_id]))
            webhook_logger.info(f"Triggering ESEA team update for teams: {team_ids}")

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
        webhook_logger.warning("Twitch signature verification failed.")
        abort(403)
    
    webhook_logger.debug("Twitch signature verified successfully.")
    
    message_type = request.headers.get(TWITCH_MESSAGE_TYPE, '').lower()
    data = request.get_json()
    
    webhook_logger.debug(f"Twitch webhook payload: {data}")
    
    # Handle different message types
    if message_type == MESSAGE_TYPE_VERIFICATION:
        challenge = data.get("challenge")
        webhook_logger.info("Received Twitch verification challenge.")
        return Response(challenge, status=200, content_type="text/plain")

    elif message_type == MESSAGE_TYPE_NOTIFICATION:
        subscription_type = data["subscription"]["type"]
        event_data = data["event"]
        user_id = event_data.get("broadcaster_user_id")
        webhook_logger.info(f"Received Twitch notification for event type: {subscription_type} | id: {user_id}")
        
        if subscription_type in ["stream.online", "stream.offline"]:
            # Handle stream online event
            webhook_logger.info(f"Processing stream status change for user ID: {event_data.get('broadcaster_user_id')}")
            eventlet.spawn(run_async(update_streamers, [user_id]))
   
        else:
            webhook_logger.warning(f"Unhandled Twitch subscription type: {subscription_type}")
            
        # Respond quickly so Twitch doesn’t time out
        return "", 204

    elif message_type == MESSAGE_TYPE_REVOCATION:
        webhook_logger.info(f"Twitch notification revoked for type: {data['subscription']['type']}")
        webhook_logger.info(f"Reason: {data['subscription']['status']}")
        webhook_logger.info(f"Condition: {data['subscription']['condition']}")
        return "", 204

    else:
        webhook_logger.error(f"Unknown Twitch message type received: {message_type}")
        return "", 204
    
    
