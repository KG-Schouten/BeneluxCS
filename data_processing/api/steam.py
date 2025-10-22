# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import aiohttp
import asyncio
import random

class SteamData:
    """The Data API for Steam"""
    
    def __init__(self, api_key, max_retries: int = 10, backoff_base: float = 1.5):
        """
        :param api_key: Your Steam Web API key
        """
        self.api_key = api_key
        self.base_url = "https://api.steampowered.com"
        self.session = None
        self.max_retries = max_retries
        self.backoff_base = backoff_base

        self.headers = {
            "Accept": "application/json"
        }
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()
    
    async def _get(self, endpoint: str, params: dict | None = None) -> dict:
        """Helper method for GET requests with error handling."""
        if self.session is None:
            raise RuntimeError("Session is not initialized. Use with `async with` block.")

        if params is None:
            params = {}

        params["key"] = self.api_key
        url = f"{self.base_url}/{endpoint}"

        for attempt in range(self.max_retries):
            async with self.session.get(url, headers=self.headers, params=params) as response:
                content_type = response.headers.get("Content-Type", "")
                is_json = "application/json" in content_type

                if response.status == 200:
                    if is_json:
                        return await response.json()
                    else:
                        body = await response.text()
                        return {}

                elif response.status == 429:
                    wait = self.backoff_base ** attempt + random.uniform(0, 0.5)
                    await asyncio.sleep(wait)

                elif 500 <= response.status < 600:
                    wait = self.backoff_base ** attempt + random.uniform(0, 0.5)
                    await asyncio.sleep(wait)

                elif response.status == 401:
                    if is_json:
                        return await response.json()
                    else:
                        return {}

                else:
                    if is_json:
                        return await response.json()
                    else:
                        body = await response.text()
                        return {}

        raise Exception(f"[SteamData] Failed after {self.max_retries} retries.")
    
    # === Steam API Methods ===

    async def get_player_summaries(self, steam_ids: list[str]) -> dict:
        """
        Fetch profile details for one or more Steam user IDs.

        :param steam_ids: List of 64-bit Steam IDs
        :return: Dictionary containing player summaries
        """
        endpoint = "ISteamUser/GetPlayerSummaries/v2/"
        params = {
            "steamids": ",".join(steam_ids)
        }
        return await self._get(endpoint, params)
    
    async def get_friend_list(self, steam_id: str, relationship: str = "all") -> dict:
        """
        Fetch the friend list for a given Steam user ID.

        :param steam_id: 64-bit Steam ID of the user
        :param relationship: Relationship type (default is 'all')
        :return: Dictionary containing friend list
        """
        endpoint = "ISteamUser/GetFriendList/v1/"
        params = {
            "steamid": steam_id,
            "relationship": relationship
        }
        return await self._get(endpoint, params)