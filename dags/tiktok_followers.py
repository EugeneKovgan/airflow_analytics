import sys
import os
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from common.common_functions import close_mongo_connection, get_mongo_client, handle_parser_error, log_parser_finish, log_parser_start 
import pendulum
from tikapi import TikAPI
from typing import Any


def get_tiktok_followers_stats(**kwargs: Any) -> None:
    parser_name = 'Tiktok Followers'
    status = 'success'
    start_time = pendulum.now()
    log_parser_start(parser_name)

    data = None
    api_key = Variable.get("TIKAPI_KEY")
    auth_key = Variable.get("TIKAPI_AUTHKEY")

    db = get_mongo_client()

    try:
        tiktok_client = TikAPI(api_key)
        user = tiktok_client.user(accountKey=auth_key)
        data = fetch_tiktok_followers_data(user, parser_name)
        if data:
            save_followers_data(db, data)
    except Exception as error:
        status = handle_parser_error(error, parser_name)
        print(f"{parser_name}: Error during processing: {error}")
    finally:
        total_followers = data.get('follower_num', {}).get('value', 0) if data else 0
        end_time = pendulum.now()
        time_taken = (end_time - start_time).total_seconds()
        save_parser_history(db, parser_name, start_time, total_followers, status, time_taken)
        close_mongo_connection(db.client)
        log_parser_finish(parser_name)

def fetch_tiktok_followers_data(user: Any, parser_name: str) -> dict:
    data = None
    retry_count = 3
    while retry_count:
        try:
            response = user.analytics(type='followers')
            data = response.json()
            print(f"Fetched TikTok followers data: {data}")
            break
        except Exception as e:
            retry_count -= 1
            status = handle_parser_error(e, parser_name)
            if status != 'success' or retry_count == 0:
                raise e
    return data

def save_followers_data(db: Any, data: dict) -> None:
    followers_collection = db['tiktok_followers']
    followers_collection.insert_one({
        'data': data,
        'recordCreated': pendulum.now(),
    })
    print(f"Saving followers data: {data}")


def save_parser_history(db: Any, parser_name: str, start_time: pendulum.DateTime, total_items: int, status: str, time_taken: float) -> None:
    end_time = pendulum.now()
    parser_history = {
        'parserName': parser_name,
        'parserStart': start_time,
        'recordCreated': end_time,
        'time': time_taken,
        'status': status,
        'followers': total_items,
    }
    db['parser_history'].insert_one(parser_history)
    print(f"Function finished with status: {status}")

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
