# common/get_instagram_access_token.py

from common.common_functions import get_mongo_client

def get_instagram_access_token() -> str:
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


