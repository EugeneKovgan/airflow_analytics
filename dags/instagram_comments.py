import sys
import os
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pendulum
from typing import Any, Dict
from pymongo import MongoClient
from common.common_functions import (
    close_mongo_connection,
    log_parser_finish,
    log_parser_start,
    save_parser_history,
    handle_parser_error,
    get_mongo_client
)
from common.get_instagram_access_token import get_instagram_access_token

def fetch_video_ids(db: MongoClient) -> list:
    posts_collection = db['instagram_reels']
    return list(posts_collection.distinct('video.id'))

def process_video_comments(db: MongoClient, video_id: str, access_token: str) -> int:
    comments_collection = db['instagram_comments']
    url = f"https://graph.facebook.com/v15.0/{video_id}/comments"
    params = {
        "fields": "from,hidden,id,text,parent_id,timestamp,user,username,like_count,replies",
        "access_token": access_token
    }

    comments_count = 0
    while url:
        response = requests.get(url, params=params)
        response.raise_for_status()
        comments = response.json().get('data', [])
        paging = response.json().get('paging', {})

        for comment in comments:
            comment['aweme_id'] = video_id
            existing_comment = comments_collection.find_one({'data.id': comment['id']})

            if existing_comment:
                comments_collection.update_one(
                    {'data.id': comment['id']},
                    {"$set": {"data": comment, "recordUpdated": pendulum.now()}}
                )
            else:
                comments_collection.insert_one({"data": comment, "recordCreated": pendulum.now()})

            comments_count += 1

        url = paging.get('next')

    return comments_count

def get_instagram_comments(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'Instagram Comments'
    status = 'success'
    start_time = pendulum.now()
    log_parser_start(parser_name)

    db = get_mongo_client()
    total_comments_count = 0

    try:
        ig_access_token = get_instagram_access_token()
        video_ids = fetch_video_ids(db)

        for video_id in video_ids:
            try:
                total_comments_count += process_video_comments(db, video_id, ig_access_token)
            except Exception as error:
                status = handle_parser_error(error, parser_name)
                if status == 'error':
                    break

    except Exception as error:
        status = handle_parser_error(error, parser_name)
    finally:
        if db:
            save_parser_history(db, parser_name, start_time, 'comments', total_comments_count, status)
        close_mongo_connection(db.client)
        log_parser_finish(parser_name)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'instagram_comments',
    default_args=default_args,
    description='Fetch Instagram comments and save to MongoDB',
    schedule_interval='35 8,21 * * *',  # Cron expression for scheduling
    start_date=days_ago(1),
    catchup=False, 
)

instagram_comments_task = PythonOperator(
    task_id='get_instagram_comments',
    python_callable=get_instagram_comments,
    dag=dag,
)

instagram_comments_task
