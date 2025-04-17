# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import os
import random
import pandas as pd
from datetime import datetime, timedelta
import re
import time
from typing import Dict, Any, Callable, Optional

# AI imports
from model.model_utils import load_model, predict, preprocess_data

# API imports
import asyncio
from faceit_api.faceit_v4 import FaceitData
from faceit_api.faceit_v1 import FaceitData_v1
from functions import load_api_keys

## Data paths
team_data_path = "C:/Python codes/BeneluxCS/DiscordBot/data/team_data.json"

if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))