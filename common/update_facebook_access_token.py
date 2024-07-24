import sys
import os
from dotenv import load_dotenv

dotenv_path = os.path.join('/mnt/e/Symfa/airflow_analytics', '.env')
load_dotenv(dotenv_path)
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from pymongo import MongoClient
import requests
import pendulum
from datetime import datetime
from typing import Dict, Any

def update_facebook_access_token() -> str:
    app_id = os.getenv('FB_APP_CLIENT_ID')
    app_secret = os.getenv('FB_APP_SECRET')
    auth_code = os.getenv('IG_AUTHCODE')

    if not all([app_id, app_secret, auth_code]):
        raise ValueError("One or more environment variables are missing.")

    db = get_mongo_client()
    settings_collection = db['settings']
    current_access_token_record = settings_collection.find_one({'name': 'instagramAccess'})

    old_access_token = ''
    old_expiration_date = ''

    if current_access_token_record and 'value' in current_access_token_record:
        old_access_token = current_access_token_record['value'].get('accessToken', '')
        old_expiration_date = pendulum.from_timestamp(
            current_access_token_record['value']['updatedIn'] + current_access_token_record['value']['expiresIn']
        ).to_datetime_string()

    is_token_expired = not current_access_token_record or (
        pendulum.now().int_timestamp > current_access_token_record['value']['updatedIn'] +
        current_access_token_record['value']['expiresIn'] - 60 * 60 * 24 * 30
    )

    new_access_token = old_access_token
    new_expiration_date = old_expiration_date

    if is_token_expired:
        auth_token = old_access_token or auth_code
        token_exchange_url = (
            f"https://graph.facebook.com/v10.0/oauth/access_token?"
            f"client_id={app_id}&client_secret={app_secret}&grant_type=fb_exchange_token&fb_exchange_token={auth_token}"
        )
        try:
            response = requests.get(token_exchange_url)
            print("Token Exchange URL:", token_exchange_url)
            print("Token Exchange Response:", response.json())

            if response.status_code != 200 or 'error' in response.json():
                raise Exception(f"Token exchange failed: {response.json()}")

            access_token_info = response.json()
            new_access_token = access_token_info['access_token']
            new_updated_in = pendulum.now().int_timestamp
            new_expires_in = access_token_info['expires_in']
            new_expiration_date = pendulum.from_timestamp(new_updated_in + new_expires_in).to_datetime_string()

            settings_collection.update_one(
                {'name': 'instagramAccess'},
                {
                    '$set': {
                        'name': 'instagramAccess',
                        'value': {
                            'accessToken': new_access_token,
                            'expiresIn': new_expires_in,
                            'updatedIn': new_updated_in,
                        },
                    }
                },
                upsert=True
            )

            save_token_update_history(
                db, 'Facebook', old_access_token, new_access_token, old_expiration_date, new_expiration_date, datetime.utcnow(), 'Success'
            )

            os.environ['IG_ACCESS_TOKEN'] = new_access_token  # Ensure this is set
            print(f"New IG_ACCESS_TOKEN set: {new_access_token}")

        except Exception as error:
            save_token_update_history(
                db, 'Facebook', old_access_token, '', old_expiration_date, '', datetime.utcnow(), 'Failed'
            )
            raise error

    print(f"Returning new access token: {new_access_token}")
    return new_access_token

def save_token_update_history(db, platform, old_access_token, new_access_token, old_expiration_date, new_expiration_date, update_timestamp, status):
    db.token_update_history.insert_one({
        "platform": platform,
        "oldAccessToken": old_access_token,
        "newAccessToken": new_access_token,
        "oldExpirationDate": old_expiration_date,
        "newExpirationDate": new_expiration_date,
        "updateTimestamp": update_timestamp,
        "status": status
    })

def get_mongo_client() -> MongoClient:
    mongo_url = os.getenv("MONGO_URL")
    mongo_dbname = os.getenv("MONGO_DBNAME")
    client = MongoClient(mongo_url)
    db = client[mongo_dbname]
    return db
