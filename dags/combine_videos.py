import sys
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pymongo import UpdateOne
from common.common_functions import close_mongo_connection, get_mongo_client, combine_videos_object, log_parser_finish, save_parser_history, handle_parser_error
from typing import Any, Dict
import pendulum

def extract_and_combine(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'Combine Videos'
    status = 'success'
    start_time = pendulum.now()
    total_videos = 0

    try:
        db = get_mongo_client()
        tiktok_posts = list(db.tiktok_posts.find())
        instagram_reels = list(db.instagram_reels.find())
        youtube_videos = list(db.youtube_videos.find())

        combined_data = []

        for post in tiktok_posts:
            transformed_post = combine_videos_object(post, "tiktok")
            combined_data.append(transformed_post)
        
        for reel in instagram_reels:
            transformed_reel = combine_videos_object(reel, "instagram")
            combined_data.append(transformed_reel)
        
        for video in youtube_videos:
            transformed_video = combine_videos_object(video, "youtube")
            combined_data.append(transformed_video)

        operations = []
        for document in combined_data:
            print(f"Document to upsert: {document}")
            operations.append(UpdateOne(
                {'_id': document['_id']},
                {'$set': document},
                upsert=True
            ))

        if operations:
            db.combined_videos.bulk_write(operations, ordered=False)
            total_videos = len(operations)

        print("Data combined and saved to new collection in remote MongoDB")
    except Exception as error:
        print(f"Error during processing: {str(error)}")
        status = handle_parser_error(error, parser_name)
        raise
    finally:
        save_parser_history(db, parser_name, start_time, 'videos', total_videos, status)
        close_mongo_connection(db.client)
        log_parser_finish(parser_name) 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'combine_videos',
    default_args=default_args,
    description='A DAG to combine three collections from remote MongoDB with platform field and save to a new collection in the same remote MongoDB',
    schedule_interval=None,
    start_date=days_ago(1),
)

extract_and_combine_task = PythonOperator(
    task_id='extract_and_combine',
    python_callable=extract_and_combine,
    dag=dag,
)

extract_and_combine_task
