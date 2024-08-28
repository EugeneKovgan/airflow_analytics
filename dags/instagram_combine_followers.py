import sys
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pendulum
from typing import Any, Dict
from airflow.models import Variable
from common.get_instagram_access_token import get_instagram_access_token
from common.common_functions import (
    close_mongo_connection,
    get_mongo_client,
    log_parser_finish,
    log_parser_start,
    save_parser_history,
    handle_parser_error,
)

def get_instagram_followers_combine_stats(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'Instagram Followers Combine'
    status = 'success'
    platform = 'instagram'
    start_time = pendulum.now()
    log_parser_start(parser_name)

    db = get_mongo_client()
    followers_collection = db['followers']
    data = None

    try:
        ig_access_token = get_instagram_access_token()
        print(f"IG_ACCESS_TOKEN received: {ig_access_token}")

        ig_business_account = Variable.get('IG_BUSINESS_ACCOUNT')
        print(f"IG_BUSINESS_ACCOUNT: {ig_business_account}")

        if not ig_access_token or not ig_business_account:
            raise ValueError("Missing access token or business account ID")

        url = f"https://graph.facebook.com/v15.0/{ig_business_account}"
        params = {
            "fields": "followers_count",
            "access_token": ig_access_token
        }

        print(f"Requesting URL: {url} with params: {params}")
        response = requests.get(url, params=params)
        print(f"Response Status Code: {response.status_code}")
        print(f"Response Content: {response.content}")
        response.raise_for_status()
        data = response.json()
        print(f"Instagram API response: {data}")

        if 'followers_count' not in data:
            raise ValueError("No 'followers_count' in the response")

        followers_collection.insert_one({
            "data": data,
            "recordCreated": pendulum.now(),
            "platform": platform,
        })

    except Exception as error:
        status = handle_parser_error(error, parser_name)
        if status == 'error':
            raise

    finally:
        if db and data:
            save_parser_history(
                db,
                parser_name,
                start_time,
                'followers',
                data.get('followers_count', 0),
                status
            )
        close_mongo_connection(db.client)
        log_parser_finish(parser_name)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'instagram_followers_combine',
    default_args=default_args,
    description='Fetch Instagram followers stats and save to MongoDB',
    schedule_interval='30 8,15,21 * * *',  # Cron expression for scheduling
    start_date=days_ago(1),
    catchup=False,
)

instagram_followers_combine_task = PythonOperator(
    task_id='get_instagram_followers_combine_stats',
    python_callable=get_instagram_followers_combine_stats,
    provide_context=True,
    dag=dag,
)

instagram_followers_combine_task
