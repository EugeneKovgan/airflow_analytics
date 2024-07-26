import sys
import os
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pendulum
from common.common_functions import (
    close_mongo_connection, get_mongo_client, save_parser_history,
    handle_parser_error, log_parser_start, log_parser_finish, get_tikapi_client
)
from typing import Any, Dict

def get_tiktok_posts_stats(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'Tiktok Posts'
    status = 'success'
    proceed = True
    start_time = pendulum.now()
    log_parser_start(parser_name)
    
    db = get_mongo_client()
    video_ids = set()
    cursor = None

    try:
        user = get_tikapi_client()
        posts_collection = db['tiktok_posts']
        posts_stats_collection = db['tiktok_posts_stats']

        i = 0
        size = 100
        while True:
            ids = list(posts_collection.find({}, {"_id": 1}).skip(i * size).limit(size))
            i += 1
            if ids:
                for id in ids:
                    video_ids.add(str(id["_id"]))
            else:
                break

        while proceed:
            data = None
            list_counter = 3
            while list_counter:
                try:
                    list_response = user.posts.feed(cursor=cursor, count=30)
                    data = list_response.json()
                    break
                except Exception as error:
                    list_counter -= 1
                    result = handle_parser_error(error, parser_name, proceed)
                    proceed = result["proceed"]
                    if list_counter == 0:
                        status = result["status"]
                        raise error

            if not proceed or data is None:
                break

            videos = data.get("itemList", [])
            if not videos:
                print("No videos found.")
                break

            for video in videos:
                if not video or not video.get("video"):
                    print(f"Skipping video due to missing properties: {video}")
                    continue

                print(f"Processing video {video.get('desc')} (#{video.get('id')}) (video_duration - {video['video']['duration']}s)")

                if video.get("secret"):
                    continue

                video_id = video["id"]
                if video_id not in video_ids:
                    post = {
                        "_id": video_id,
                        "video": video,
                        "recordCreated": pendulum.now(),
                        "tags": None,
                    }
                    posts_collection.insert_one(post)
                    print(f"New Video Discovered from {pendulum.from_timestamp(video['createTime'])} of {video['video']['duration']}s {video.get('desc')}")
                else:
                    posts_collection.update_one(
                        {"_id": video_id},
                        {"$set": {"video": video, "recordCreated": pendulum.now()}}
                    )

                counter_analytics = 3
                analytics = None
                while counter_analytics:
                    try:
                        analytics_response = user.analytics(type="video", media_id=video_id)
                        analytics = analytics_response.json()
                        break
                    except Exception as error:
                        counter_analytics -= 1
                        result = handle_parser_error(error, parser_name, proceed)
                        proceed = result["proceed"]
                        if counter_analytics == 0:
                            status = result["status"]
                            raise error

                if not proceed:
                    break

                posts_stats_collection.insert_one({
                    "recordCreated": pendulum.now(),
                    "postId": video_id,
                    "analytics": analytics,
                })

            proceed = data.get("hasMore", False)
            cursor = data.get("cursor")

    except Exception as error:
        result = handle_parser_error(error, parser_name, proceed)
        status = result["status"]
        proceed = result["proceed"]
    finally:
        if db:
            posts_count = len(video_ids)
            save_parser_history(db, parser_name, start_time, 'posts', posts_count, status)
            close_mongo_connection(db.client)
            log_parser_finish(parser_name)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'tiktok_posts',
    default_args=default_args,
    description='Fetch TikTok posts stats and save to MongoDB',
    schedule_interval='0 3,7,11,15,19 * * *',  # Cron expression for scheduling
    start_date=days_ago(1),
    catchup=False,
)

tiktok_posts_task = PythonOperator(
    task_id='tiktok_posts',
    python_callable=get_tiktok_posts_stats,
    provide_context=True,
    dag=dag,
)

tiktok_posts_task
