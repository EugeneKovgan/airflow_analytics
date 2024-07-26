import sys
import os
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pendulum
from datetime import datetime
from typing import Any, Dict
from common.common_functions import (
    get_mongo_client,
    close_mongo_connection,
    log_parser_finish,
    log_parser_start,
    save_parser_history,
    handle_parser_error,
)

def update_instagram_access_token(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'Update instagram Access Token'
    status = 'success'
    proceed = True
    start_time = pendulum.now()
    log_parser_start(parser_name)

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

    if is_token_expired:
        auth_token = old_access_token or auth_code
        token_exchange_url = (
            f"https://graph.instagram.com/v10.0/oauth/access_token?"
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
                db, 'instagram', old_access_token, new_access_token, old_expiration_date, new_expiration_date, datetime.utcnow(), 'Success'
            )

            os.environ['IG_ACCESS_TOKEN'] = new_access_token

        except Exception as error:
            status = 'failure'
            proceed = False
            save_token_update_history(
                db, 'instagram', old_access_token, '', old_expiration_date, '', datetime.utcnow(), 'Failed'
            )
            handle_parser_error(error, parser_name, proceed)

    save_parser_history(db, parser_name, start_time, 'access_token', 1 if proceed else 0, status)
    close_mongo_connection(db.client)
    log_parser_finish(parser_name)

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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'instagram_update_access_token',
    default_args=default_args,
    description='Fetch and update instagram access token',
    schedule_interval='@daily',
    start_date=days_ago(1),
)

update_token_task = PythonOperator(
    task_id='update_instagram_access_token',
    python_callable=update_instagram_access_token,
    provide_context=True,
    dag=dag,
)

update_token_task
