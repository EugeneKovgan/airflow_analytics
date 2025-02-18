import sys
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
    platform = 'tiktok'
    start_time = pendulum.now()
    log_parser_start(parser_name)
    
    db = get_mongo_client()
    video_ids = set()
    cursor = None

    try:
        user = get_tikapi_client()
        posts_collection = db['tiktok_posts']
        posts_stats_collection = db['tiktok_posts_stats']

        result = posts_collection.update_many(
            {"platform": {"$exists": False}},
            {"$set": {"platform": platform}}
        )
        print(f"Documents updated: {result.modified_count}")

        i = 0
        size = 100
        while True:
            ids = list(posts_collection.find({}, {"_id": 1}).skip(i * size).limit(size))
            i += 1
            if ids:
                for id_doc in ids:
                    video_ids.add(str(id_doc["_id"]))
            else:
                break

        while True:
            data = None
            list_counter = 3
            while list_counter:
                try:
                    list_response = user.posts.feed(cursor=cursor, count=30)
                    data = list_response.json()
                    break
                except Exception as error:
                    list_counter -= 1
                    status = handle_parser_error(error, parser_name)
                    if list_counter == 0:
                        raise error

            if data is None:
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
                        "recordCreated": pendulum.now(),
                        "tags": None,
                        "video": video,
                        "platform": platform,
                    }
                    posts_collection.insert_one(post)
                    print(f"New Video Discovered from {pendulum.from_timestamp(video['createTime'])} of {video['video']['duration']}s {video.get('desc')}")
                else:
                    posts_collection.update_one(
                        {"_id": video_id},
                        {"$set": {"recordCreated": pendulum.now(), "video": video, "platform": platform }}
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
                        status = handle_parser_error(error, parser_name)
                        if counter_analytics == 0:
                            raise error

                posts_stats_collection.insert_one({
                    "recordCreated": pendulum.now(),
                    "postId": video_id,
                    "analytics": analytics,
                })

            if not data.get("hasMore", False):
                break

            cursor = data.get("cursor")

    except Exception as error:
        status = handle_parser_error(error, parser_name)
    finally:
        if db:
            posts_count = len(video_ids)
            save_parser_history(db, parser_name, start_time, 'videos', posts_count, status)
            close_mongo_connection(db.client)
            log_parser_finish(parser_name)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    '_tiktok_posts_',
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
