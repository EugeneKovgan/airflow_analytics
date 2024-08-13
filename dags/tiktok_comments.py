import sys
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pendulum
from common.common_functions import (
    close_mongo_connection,
    handle_parser_error,
    log_parser_finish,
    log_parser_start,
    save_parser_history,
    get_mongo_client
)
from airflow.models import Variable
from tikapi import TikAPI
from typing import Any, Dict, List

def get_tiktok_comments(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'Tiktok Comments'
    status = 'success'
    start_time = pendulum.now()
    log_parser_start(parser_name)

    total_comments_count = 0
    platform = 'tiktok'

    try:
        db = get_mongo_client()
        api_key = Variable.get("TIKAPI_KEY")
        auth_key = Variable.get("TIKAPI_AUTHKEY")
        tiktok_client = TikAPI(api_key)
        user = tiktok_client.user(accountKey=auth_key)

        video_ids = fetch_video_ids(db)
        for video_id in video_ids:
            try:
                total_comments_count += process_video_comments(user, db, video_id, platform)
            except Exception as error:
                status = handle_parser_error(error, parser_name)
                print(f"{parser_name}: Error processing comments for video {video_id}: {error}")
                break  # Exit the loop on a critical error

    except Exception as error:
        status = handle_parser_error(error, parser_name)
        print(f"{parser_name}: Error: {error}")

    finally:
        if db:
            save_parser_history(db, parser_name, start_time, 'comments', total_comments_count, status)
        close_mongo_connection(db.client)
        log_parser_finish(parser_name)

def fetch_video_ids(db: Any) -> List[str]:
    posts_collection = db['tiktok_posts']
    return list(posts_collection.distinct('video.id'))

def process_video_comments(user: Any, db: Any, video_id: str, platform: str) -> int:
    total_comments_count = 0

    try:
        response = user.posts.comments.list(media_id=video_id)
        if 'comments' in response.json():
            comments_collection = db['tiktok_comments']
            for comment in response.json()['comments']:
                total_comments_count += process_comment(user, comments_collection, comment, platform)
        else:
            print(f"No comments found for video {video_id}")
    except Exception as error:
        raise error

    return total_comments_count

def process_comment(user: Any, comments_collection: Any, comment: Any, platform: str) -> int:
    existing_comment = comments_collection.find_one({ 'data.cid': comment['cid'] })

    if not existing_comment:
        reply_comments = fetch_reply_comments(user, comment) if comment.get('reply_comment_total', 0) > 0 else []
        comments_collection.insert_one({
            'data': comment,
            'recordCreated': pendulum.now(),
            'reply': reply_comments,
            'platform': platform,
        })
        return 1
    else:
        updated_reply_comments = existing_comment.get('reply', [])
        if existing_comment['data'].get('reply_comment_total') != comment.get('reply_comment_total'):
            updated_reply_comments = fetch_reply_comments(user, comment) if comment.get('reply_comment_total', 0) > 0 else []

        update_fields = {
            'data': comment,
            'recordCreated': pendulum.now(),
            'reply': updated_reply_comments,
        }

        if 'platform' not in existing_comment:
            update_fields['platform'] = platform

        comments_collection.replace_one(
            { 'data.cid': comment['cid'] },
            update_fields,
        )

        return 0

def fetch_reply_comments(user: Any, comment: Any) -> List[Dict[str, Any]]:
    response_reply = user.posts.comments.replies(
        media_id=comment['aweme_id'], 
        comment_id=comment['cid']
    )
    print(f"Fetched reply comments for comment {comment['cid']}: {response_reply.json()['comments']}")
    return response_reply.json()['comments']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'tiktok_comments',
    default_args=default_args,
    description='Fetch TikTok comments and save to MongoDB',
    schedule_interval='35 7,19 * * *',   # Cron expression for scheduling
    start_date=days_ago(1),
    catchup=False,
)

tiktok_comments_task = PythonOperator(
    task_id='get_tiktok_comments',
    python_callable=get_tiktok_comments,
    provide_context=True,
    dag=dag,
)

tiktok_comments_task
