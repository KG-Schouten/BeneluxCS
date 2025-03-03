# Benelux Counter-Strike Bot 🇧🇪🇳🇱🇱🇺

## Your all-in-one CS bot for the Benelux scene! 🏆

The **Benelux Counter-Strike Bot** is designed to provide ESEA match updates, LAN event information, and Benelux Hub stats directly in Discord.

## Setup and Configuration
### Set Up API Tokens  
This bot requires API tokens from Faceit and Discord to fetch data from these services

#### 🔧 **Steps to configure API tokens:**  
1. **Copy the example api_keys file:**  
   ```sh
   cd tokens
   cp api_keys.example.json api_keys.json

2. **Open ``tokens/api_keys.json`` and fill in your API keys:**  
   ```sh
   {
    "FACEIT_TOKEN": "your-faceit-token-here",
    "DISCORD_TOKEN": "your-discord-token-here"
    }
