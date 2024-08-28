import sys
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pendulum
from typing import Any, Dict

from common.common_functions import (
    close_mongo_connection,
    log_parser_finish,
    log_parser_start,
    save_parser_history,
    handle_parser_error,
    get_mongo_client
)
from airflow.models import Variable
from common.get_instagram_access_token import get_instagram_access_token

def get_instagram_reels_combine_stats(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'Instagram Reels Combine Stats'
    status = 'success'
    platform = 'instagram'
    start_time = pendulum.now()
    log_parser_start(parser_name)

    reels_ids = set()
    db = get_mongo_client()
    reels_collection = db['videos'] 
    reels_stats_collection = db['instagram_reels_stats']
    data = None

    try:
        ig_access_token = get_instagram_access_token()
        print(f"IG_ACCESS_TOKEN received: {ig_access_token}")

        ig_business_account = Variable.get('IG_BUSINESS_ACCOUNT').strip()
        print(f"IG_BUSINESS_ACCOUNT: {ig_business_account}")

        if not ig_access_token or not ig_business_account:
            raise ValueError("Missing access token or business account ID")

        followers_url = f"https://graph.facebook.com/v14.0/{ig_business_account}"
        followers_params = {
            'fields': 'followers_count,id,profile_picture_url',
            'access_token': ig_access_token
        }

        followers_response = requests.get(followers_url, params=followers_params)
        print(followers_response.json())
        if followers_response.status_code != 200:
            raise Exception(f"API request for followers count failed with status {followers_response.status_code}: {followers_response.json()}")

        followers_data = followers_response.json()
        followers_count = followers_data.get('followers_count', 0)
        profile_picture_id = followers_data.get('id', '')
        profile_picture_url = followers_data.get('profile_picture_url', '')

        i = 0
        size = 100
        while True:
            ids = list(reels_collection.find({"platform": platform}, {"_id": 1}).skip(i * size).limit(size))
            if not ids:
                break
            for id_doc in ids:
                reels_ids.add(id_doc["_id"])
            i += 1

        url = f"https://graph.facebook.com/v14.0/{ig_business_account}/media"
        params = {
            'fields': 'like_count,comments_count,media_type,timestamp,media_product_type,shortcode,permalink,caption,thumbnail_url,is_comment_enabled,is_shared_to_feed,text,media_url,profile_picture_url,username',
            'access_token': ig_access_token
        }

        while True:
            response = requests.get(url, params=params)
            if response.status_code != 200:
                raise Exception(f"API request failed with status {response.status_code}: {response.json()}")

            media = response.json()
            reels = parse_reels_data(media['data'], ig_access_token)

            if reels:
                for reel in reels:
                    id = reel['id']
                    # Upsert a new reel or update existing one
                    new_reel_post = {
                        "video": {
                            "id": id,
                            "author": reel.get('username'),
                            "desc": reel.get('caption', '').split('\n')[0],
                            "type": reel['media_type'],
                            "cover": reel['thumbnail_url'],
                            "createTime": pendulum.parse(reel['timestamp']).int_timestamp,
                            "url": reel['permalink'],
                            "text": reel.get('text', ''),
                            "profile_picture_url": {
                                "profile_picture_url": profile_picture_url,
                                "followers_count": followers_count,
                                "id": profile_picture_id
                            },
                            "image_url": reel.get('media_url', '')
                        },
                        "platform": platform  
                    }
                    
                    update_data = {
                        "$setOnInsert": {
                            "recordCreated": pendulum.now()
                        },
                        "$set": {
                            "tags": None,
                            "video": new_reel_post["video"],
                            "platform": platform 
                        }
                    }
                    reels_collection.update_one({"_id": id}, update_data, upsert=True)

                    print(f"Video Processed: {new_reel_post['video']['desc']}")

                    reels_stats_collection.insert_one({
                        "postId": id,
                        "recordCreated": pendulum.now(),
                        "platform": platform, 
                        "statistics": {
                            "like_count": reel['like_count'],
                            "comment_count": reel['comments_count'],
                            "reach": reel.get('reach', 0),
                            "collect_count": reel.get('saved', 0),
                            "share_count": reel.get('shares', 0),
                            "play_count": reel.get('plays', 0),
                            "interactions": reel.get('total_interactions', 0),
                            "is_comment_enabled": reel['is_comment_enabled'],
                            "is_shared_to_feed": reel['is_shared_to_feed'],
                            "shortcode": reel['shortcode'],
                            "followers": {
                                "followers_count": followers_count,
                                "id": profile_picture_id
                            },
                            "profile_picture_url": reel.get('profile_picture_url', ''),
                        }
                    })

            # Check if there's a next page
            if 'paging' in media and 'next' in media['paging']:
                url = media['paging']['next']
            else:
                break

    except Exception as error:
        status = handle_parser_error(error, parser_name)
        if status == 'error':
            raise
    finally:
        if db:
            save_parser_history(
                db,
                parser_name,
                start_time,
                'videos',
                len(reels_ids),
                status
            )
        close_mongo_connection(db.client)
        log_parser_finish(parser_name)

def parse_reels_data(media, access_token):
    data = []
    for item in media:
        if item['media_type'] == 'VIDEO' and item['media_product_type'] == 'REELS':
            item_data = item
            metrics_url = f"https://graph.facebook.com/v14.0/{item['id']}/insights"
            metrics_params = {
                'metric': 'reach,saved,shares,plays,total_interactions',
                'access_token': access_token
            }
            metrics_response = requests.get(metrics_url, params=metrics_params)
            if metrics_response.status_code == 200:
                metrics = metrics_response.json().get('data', [])
                for metric in metrics:
                    item_data[metric['name']] = metric['values'][0]['value']
            data.append(item_data)
    return data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'instagram_reels_combine',
    default_args=default_args,
    description='Fetch Instagram Reels stats and save to MongoDB',
    schedule_interval='0 8,15,21 * * *',  # Cron expression for scheduling
    start_date=days_ago(1),
)

instagram_reels_combine_stats_task = PythonOperator(
    task_id='get_instagram_reels_combine_stats',
    python_callable=get_instagram_reels_combine_stats,
    provide_context=True,
    dag=dag,
)

instagram_reels_combine_stats_task
