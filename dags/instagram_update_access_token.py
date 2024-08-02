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
from airflow.models import Variable
from common.common_functions import (
    get_mongo_client,
    close_mongo_connection,
    log_parser_start,
    log_parser_finish,
    save_parser_history,
    handle_parser_error,
    save_token_update_history
)

def update_instagram_access_token(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'Update Instagram Access Token'
    status = 'success'
    start_time = pendulum.now()
    log_parser_start(parser_name)

    app_id = Variable.get('FB_APP_CLIENT_ID')
    app_secret = Variable.get('FB_APP_SECRET')
    auth_code = Variable.get('IG_AUTHCODE')

    if not all([app_id, app_secret, auth_code]):
        raise ValueError("One or more environment variables are missing.")

    db = get_mongo_client()
    settings_collection = db['settings']

    current_access_token_record = settings_collection.find_one({'name': 'instagramAccess'})
    old_access_token = current_access_token_record['value']['accessToken'] if current_access_token_record else ''
    old_expiration_date = ''

    if current_access_token_record:
        old_expiration_date = pendulum.from_timestamp(
            current_access_token_record['value']['updatedIn'] + current_access_token_record['value']['expiresIn']
        ).to_datetime_string()
        print(f"Current token expires on: {old_expiration_date}")

    current_time = pendulum.now().int_timestamp
    token_lifetime = current_access_token_record['value']['expiresIn'] if current_access_token_record else 0
    token_updated_time = current_access_token_record['value']['updatedIn'] if current_access_token_record else 0
    token_expiry_time = token_updated_time + token_lifetime
    refresh_time_threshold = token_expiry_time - 60 * 60 * 24 * 10

    is_token_expired = not current_access_token_record or current_time >= refresh_time_threshold

    print(f"Is token expired? {is_token_expired}")

    try:
        if not current_access_token_record:
            # No token found, request new token using IG_AUTHCODE
            print('No token found, requesting new token using IG_AUTHCODE...')
            token_exchange_url = (
                f"https://graph.facebook.com/oauth/access_token?"
                f"client_id={app_id}&client_secret={app_secret}&grant_type=fb_exchange_token&fb_exchange_token={auth_code}"
            )
            response = requests.get(token_exchange_url)
            response.raise_for_status()
            response_data = response.json()

            new_access_token = response_data['access_token']
            new_updated_in = pendulum.now().int_timestamp
            new_expires_in = response_data['expires_in']
            new_expiration_date = pendulum.from_timestamp(new_updated_in + new_expires_in).to_datetime_string()

            settings_collection.update_one(
                {'name': 'instagramAccess'},
                {'$set': {
                    'name': 'instagramAccess',
                    'value': {
                        'accessToken': new_access_token,
                        'expiresIn': new_expires_in,
                        'updatedIn': new_updated_in,
                    }
                }},
                upsert=True
            )

            save_token_update_history(db, 'Instagram', old_access_token, new_access_token, old_expiration_date, new_expiration_date, datetime.utcnow(), 'Success')

        elif is_token_expired:
            # Token is expired, refresh token
            print('Token expired, attempting to refresh...')
            token_refresh_url = (
                f"https://graph.instagram.com/refresh_access_token?"
                f"grant_type=ig_refresh_token&access_token={old_access_token}"
            )
            response = requests.get(token_refresh_url)
            response.raise_for_status()
            response_data = response.json()

            new_access_token = response_data['access_token']
            new_updated_in = pendulum.now().int_timestamp
            new_expires_in = response_data['expires_in']
            new_expiration_date = pendulum.from_timestamp(new_updated_in + new_expires_in).to_datetime_string()

            settings_collection.update_one(
                {'name': 'instagramAccess'},
                {'$set': {
                    'name': 'instagramAccess',
                    'value': {
                        'accessToken': new_access_token,
                        'expiresIn': new_expires_in,
                        'updatedIn': new_updated_in,
                    }
                }},
                upsert=True
            )

            save_token_update_history(db, 'Instagram', old_access_token, new_access_token, old_expiration_date, new_expiration_date, datetime.utcnow(), 'Success')

        else:
            print('Token is still valid, no need to refresh.')

    except requests.exceptions.RequestException as error:
        print('Error during token update:', error)
        save_token_update_history(db, 'Instagram', old_access_token, '', old_expiration_date, '', datetime.utcnow(), 'Failed', str(error))
        status = handle_parser_error(error, parser_name)

    finally:
        save_parser_history(db, parser_name, start_time, 'access_token', 1, status)
        close_mongo_connection(db.client)
        log_parser_finish(parser_name)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'instagram_update_access_token',
    default_args=default_args,
    description='Fetch and update Instagram access token',
    schedule_interval='@daily',
    start_date=days_ago(1),
)

update_token_task = PythonOperator(
    task_id='update_instagram_access_token',
    python_callable=update_instagram_access_token,
    dag=dag,
)

update_token_task
