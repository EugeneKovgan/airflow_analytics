import sys
import os
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pymongo import MongoClient
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from common.common_functions import (
    close_mongo_connection,
    handle_parser_error,
    log_parser_finish,
    log_parser_start,
    save_parser_history,
    get_mongo_client
)
import requests
import os
import pendulum
from typing import Any, Dict

MAX_RETRIES = 3

def update_facebook_access_token() -> str:
    app_id = os.getenv('FB_APP_CLIENT_ID')
    app_secret = os.getenv('FB_APP_SECRET')
    auth_code = os.getenv('IG_AUTHCODE')

    token_exchange_url = f'https://graph.facebook.com/v10.0/oauth/access_token?client_id={app_id}&client_secret={app_secret}&code={auth_code}'
    response = requests.get(token_exchange_url)
    access_token_info = response.json()
    return access_token_info['access_token']

def fetch_instagram_followers_data() -> Dict[str, Any]:
    ig_access_token = update_facebook_access_token()
    FacebookAdsApi.init(ig_access_token)
    account = User(os.getenv('IG_BUSINESS_ACCOUNT'))
    try:
        data = account.api_get(fields=['followers_count'])
        return data
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        raise

def save_data_to_mongo(collection_name: str, data: Dict[str, Any], ts: str) -> None:
    db = get_mongo_client()
    db[collection_name].insert_one({
        "data": data,
        "recordCreated": ts
    })

def save_followers_data(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'Instagram Followers'
    status = 'success'
    proceed = True
    start_time = pendulum.now()
    log_parser_start(parser_name)
    total_followers = 0
    attempt = 0
    data = None

    try:
        db = get_mongo_client()
        while attempt < MAX_RETRIES:
            try:
                data = fetch_instagram_followers_data()
                ts = kwargs['ts']
                if not isinstance(ts, str):
                    raise ValueError("Timestamp 'ts' must be a string")
                if data:
                    save_data_to_mongo('instagram_followers', data, ts)
                    total_followers = data['followers_count']
                break
            except Exception as error:
                attempt += 1
                if attempt >= MAX_RETRIES:
                    result = handle_parser_error(error, parser_name, MAX_RETRIES)
                    status = result["status"]
                    proceed = result["proceed"]
                    if not proceed:
                        raise Exception(f"Critical error: {error}")
                else:
                    result = handle_parser_error(error, parser_name, MAX_RETRIES)
                    status = result["status"]
                    proceed = result["proceed"]
                    if not proceed:
                        break

    except Exception as error:
        status = 'failure'
        print(f"Error during processing: {str(error)}")
        raise
    finally:
        save_parser_history(db, parser_name, start_time, 'followers', total_followers, status)
        close_mongo_connection(db.client)
        log_parser_finish(parser_name)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'instagram_followers',
    default_args=default_args,
    description='Fetch Instagram followers stats and save to MongoDB',
    schedule_interval=None,
    start_date=days_ago(1),
)

instagram_followers_task = PythonOperator(
    task_id='get_instagram_followers_stats',
    python_callable=save_followers_data,
    provide_context=True,
    dag=dag,
)

instagram_followers_task
