import mysql.connector
import json
import re
import datetime
import pandas as pd
import numpy as np
from typing import *

from functions import load_api_keys
from database.db_config import db_name

def start_database():
    """Starting up the mySQL database so it can be used by the rest of the program"""

    global db_name
    ## Load password for MySQL database
    api_keys = load_api_keys()
    mysql_password = api_keys["MYSQL_PASSWORD"]

    db = mysql.connector.connect(
        host="localhost",
        user="Koen",
        passwd=mysql_password,
        database=db_name
    )
    cursor = db.cursor()

    return db, cursor

def close_database(db, cursor):
    """Closing the database connection"""
    db.close()
    cursor.close()