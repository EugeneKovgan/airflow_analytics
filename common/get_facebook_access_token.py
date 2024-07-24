import sys
import os
from dotenv import load_dotenv

dotenv_path = os.path.join('/mnt/e/Symfa/airflow_analytics', '.env')
load_dotenv(dotenv_path)
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from pymongo import MongoClient
import pendulum

def get_facebook_access_token() -> str:
    db = get_mongo_client()
    settings_collection = db['settings']
    current_access_token_record = settings_collection.find_one({'name': 'instagramAccess'})

    if not current_access_token_record or 'value' not in current_access_token_record:
        raise ValueError("No access token found in the database")

    access_token = current_access_token_record['value'].get('accessToken', '')

    if not access_token:
        raise ValueError("Access token is empty in the database")

    print(f"Returning access token from database: {access_token}")
    return access_token

def get_mongo_client() -> MongoClient:
    mongo_url = os.getenv("MONGO_URL")
    mongo_dbname = os.getenv("MONGO_DBNAME")
    client = MongoClient(mongo_url)
    db = client[mongo_dbname]
    return db
