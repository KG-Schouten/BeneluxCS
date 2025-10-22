# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import requests
import json

from dotenv import load_dotenv
load_dotenv()

# --- Twitch User Functions ---
def get_twitch_streamer_info(streamer_ids: list = [], streamer_names: list = []):
    if not isinstance(streamer_names, list) or not all(isinstance(name, str) for name in streamer_names):
        raise ValueError("Invalid streamer_names. It should be a list of strings.")
    
    if not isinstance(streamer_ids, list) or not all(isinstance(sid, str) for sid in streamer_ids):
        raise ValueError("Invalid streamer_ids. It should be a list of strings.")

    params = {}
    if streamer_ids:
        params["id"] = streamer_ids
    if streamer_names:
        params["login"] = streamer_names
    
    try:
        response = requests.get(
            "https://api.twitch.tv/helix/users",
            headers={
                "Client-ID": os.getenv("TWITCH_CLIENT_ID"),
                "Authorization": f"Bearer {os.getenv('TWITCH_ACCESS_TOKEN')}"
            },
            params=params
        )
        
        data = response.json()
        
        if not data.get("data"):
            raise ValueError(f"No data found for streamer: {streamer_names}")
        
        return data["data"]
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# --- Twitch Stream Functions ---
def get_twitch_stream_info(streamer_ids: list = [], streamer_names: list = []) -> list:
    if not isinstance(streamer_names, list) or not all(isinstance(name, str) for name in streamer_names):
        raise ValueError("Invalid streamer_names. It should be a list of strings.")
    
    if not isinstance(streamer_ids, list) or not all(isinstance(sid, str) for sid in streamer_ids):
        raise ValueError("Invalid streamer_ids. It should be a list of strings.")

    params = {}
    if streamer_ids:
        params["user_id"] = streamer_ids
    if streamer_names:
        params["user_login"] = streamer_names
    
    try:
        response = requests.get(
            "https://api.twitch.tv/helix/streams",
            headers={
                "Client-ID": os.getenv("TWITCH_CLIENT_ID"),
                "Authorization": f"Bearer {os.getenv('TWITCH_ACCESS_TOKEN')}"
            },
            params=params
        )
        
        data = response.json()
        
        if not data.get("data"):
            raise ValueError(f"No data found for streamer: {streamer_names}")
        
        return data["data"]
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

def get_twitch_streams_benelux():
    try:
        response = requests.get(
            "https://api.twitch.tv/helix/streams",
            headers={
                "Client-ID": os.getenv("TWITCH_CLIENT_ID"),
                "Authorization": f"Bearer {os.getenv('TWITCH_ACCESS_TOKEN')}"
            },
            params={
                "game_id": '32399',
                "language": ['nl', 'be'],
                "first": 100
            }
        )
        
        data = response.json()
        
        return data.get("data", [])
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

# --- Twitch EventSub Management Functions ---
def get_twitch_eventSub_subscriptions():
    try:
        response = requests.get(
            "https://api.twitch.tv/helix/eventsub/subscriptions",
            headers={
                "Client-ID": os.getenv("TWITCH_CLIENT_ID"),
                "Authorization": f"Bearer {os.getenv('TWITCH_ACCESS_TOKEN')}"
            }
        )
        
        data = response.json()
        
        return data.get("data", [])
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

def twitch_eventsub_subscribe(streamer_ids: list):
    callback_url = "https://beneluxcs.nl" + str(os.getenv("TWITCH_WEBHOOK_URL"))
    
    try:
        event_types = ["stream.online", "stream.offline"]
        
        current_subscriptions = get_twitch_eventSub_subscriptions()
        for streamer_id in streamer_ids:
            for event_type in event_types:
                # Check if subscription already exists
                if any(sub["type"] == event_type and sub["condition"]["broadcaster_user_id"] == streamer_id for sub in current_subscriptions):
                    print(f"Subscription for {event_type} already exists for streamer {streamer_id}. Skipping.")
                    continue
                        
                payload = {
                    "type": event_type,
                    "version": "1",
                    "condition": {
                        "broadcaster_user_id": streamer_id
                    },
                    "transport": {
                        "method": "webhook",
                        "callback": callback_url,
                        "secret": os.getenv("TWITCH_SECRET")
                    }
                }

                response = requests.post(
                    "https://api.twitch.tv/helix/eventsub/subscriptions",
                    headers={
                        "Client-ID": os.getenv("TWITCH_CLIENT_ID"),
                        "Authorization": f"Bearer {os.getenv('TWITCH_ACCESS_TOKEN')}",
                        "Content-Type": "application/json"
                    },
                    data=json.dumps(payload)
                )
                
                print(response.status_code)
                print(response.json())
                    
    except Exception as e:
        print(f"An error occurred: {e}")

def twitch_eventsub_unsubscribe(subscription_ids: list):
    try:
        if not subscription_ids:
            print("No subscription IDs provided for unsubscription.")
            return
        
        for subscription_id in subscription_ids:
            url = f"https://api.twitch.tv/helix/eventsub/subscriptions?id={subscription_id}"
        
            headers = {
                "Client-ID": os.getenv("TWITCH_CLIENT_ID"),
                "Authorization": f"Bearer {os.getenv('TWITCH_ACCESS_TOKEN')}"
            }
            
            response = requests.delete(url, headers=headers)
            
            print(response.status_code)
            print(response.json())
    except Exception as e:
        print(f"An error occurred: {e}")