import sys
import os
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pendulum
from pymongo import MongoClient
from common.common_functions import (
    close_mongo_connection,
    handle_parser_error,
    log_parser_finish,
    log_parser_start,
    save_parser_history,
    get_mongo_client,
    get_tikapi_client
)
from typing import Any, Dict

MAX_RETRIES = 3

def fetch_tiktok_followers_data(tiktok_client, parser_name, proceed):
    user = tiktok_client.user({
        "accountKey": os.getenv('TIKAPI_AUTHKEY')
    })
    data = None
    retry_count = MAX_RETRIES
    while retry_count:
        try:
            data = user.analytics({
                "type": "followers"
            })
            break
        except Exception as e:
            retry_count -= 1
            result = handle_parser_error(e, parser_name, proceed)
            if not result["proceed"]:
                raise e
    return data

def save_followers_data(db, data):
    followers_collection = db.collection('tiktok_followers')
    followers_collection.insert_one({
        "data": data.json(),
        "recordCreated": pendulum.now()
    })

def get_tiktok_followers_stats(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'Tiktok Followers'
    status = 'success'
    proceed = True
    start_time = pendulum.now()
    log_parser_start(parser_name)

    mongo_client = MongoClient(os.getenv('MONGO_URL'))
    db = mongo_client[os.getenv('MONGO_DBNAME')]
    data = None

    try:
        tiktok_client = get_tikapi_client()
        data = fetch_tiktok_followers_data(tiktok_client, parser_name, proceed)
        if data:
            save_followers_data(db, data)
    except Exception as error:
        result = handle_parser_error(error, parser_name, proceed)
        status = result["status"]
        proceed = result["proceed"]
    finally:
        total_followers = data.json().get('follower_num', {}).get('value', 0) if data else 0
        save_parser_history(db, parser_name, start_time, 'followers', total_followers, status)
        close_mongo_connection(mongo_client)
        log_parser_finish(parser_name)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'tiktok_followers',
    default_args=default_args,
    description='Fetch TikTok followers stats and save to MongoDB',
    schedule_interval='30 3,7,11,15,19 * * *', # Cron expression for scheduling
    start_date=days_ago(1),
    catchup=False,
)

tiktok_followers_task = PythonOperator(
    task_id='get_tiktok_followers_stats',
    python_callable=get_tiktok_followers_stats,
    provide_context=True,
    dag=dag,
)

tiktok_followers_task
