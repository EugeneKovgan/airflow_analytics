import sys
import os
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pendulum
from common.common_functions import (
    close_mongo_connection, get_mongo_client, save_parser_history,
    handle_parser_error, log_parser_start, log_parser_finish
)
from typing import Any, Dict

def calculate_post(analytics_records: list, post_id: str):
    now = pendulum.now()
    today = pendulum.datetime(now.year, now.month, now.day)
    stats = analytics_records.pop(0) if analytics_records else None
    if not stats:
        return []
    time = pendulum.from_timestamp(stats['videoCreateTime'])

    def empty_stats(time):
        return {
            'postId': post_id,
            'date': time.format('YYYY-MM-DD') if time else '',
            'collect_count': 0,
            'comment_count': 0,
            'digg_count': 0,
            'download_count': 0,
            'forward_count': 0,
            'lose_comment_count': 0,
            'lose_count': 0,
            'play_count': 0,
            'share_count': 0,
            'whatsapp_share_count': 0,
            'finish_rate': 0,
            'recordCreated': time,
            'total_duration': 0,
            'video_duration': 0,
            'followers': 0,
        }

    analytics = []
    for record in analytics_records:
        stat = empty_stats(time)
        for key in record['statistics']:
            stat[key] = record['statistics'][key]
        analytics.append(stat)
    
    return analytics

def recalculate_tiktok_daily_stats(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'Tiktok Daily Stats'
    status = 'success'
    start_time = pendulum.now()
    log_parser_start(parser_name)
    
    followers = 0
    proceed = True
    
    try:
        db = get_mongo_client()
        posts_collection = db['tiktok_posts']
        posts_stats_collection = db['tiktok_posts_stats']
        daily_stats_collection = db['tiktok_daily_stats']
        daily_followers_collection = db['tiktok_daily_followers']

        # Check if the collection exists and create it if it doesn't
        if 'tiktok_daily_stats' not in db.list_collection_names():
            db.create_collection('tiktok_daily_stats')

        daily_followers_stats = daily_followers_collection.find()
        daily_followers_map = {x['_id']: x['followers'] for x in daily_followers_stats}
        
        daily_views_map = {}
        posts = list(posts_collection.find({}))
        
        batch_size = 10
        for i in range(0, len(posts), batch_size):
            batch = posts[i:i + batch_size]
            for p in batch:
                video_duration = p['video']['video']['duration']
                video_desc = p['video']['desc']
                print(f"Processing video {video_desc} (#{p['_id']}) (video_duration - {video_duration})")
                
                analytics_records = list(posts_stats_collection.find({'postId': p['_id']}).sort('recordCreated', 1))
                filtered_analytics_records = [x for x in analytics_records if 'video_info' in x['analytics']]
                
                analytics = calculate_post(
                    [{'statistics': x['analytics']['video_info']['statistics'], 
                    'recordCreated': pendulum.instance(x['recordCreated']), 
                    'videoCreateTime': x['analytics']['video_info']['create_time'], 
                    'finish_rate': x['analytics']['finish_rate']['value'] if 'finish_rate' in x['analytics'] else 0, 
                    'total_duration': x['analytics']['video_total_duration']['value'] if 'video_total_duration' in x['analytics'] else 0} 
                    for x in filtered_analytics_records], 
                    str(p['_id'])
                )
                
                if analytics:
                    for a in analytics:
                        a['postId'] = str(p['_id'])
                        daily_videos = daily_views_map.get(a['date'], [])
                        daily_videos.append(a)
                        daily_views_map[a['date']] = daily_videos
                        a['video_duration'] = video_duration
        
        daily_stats_collection.drop()
        
        for day, day_videos in daily_views_map.items():
            day_total_views = sum(video['play_count'] for video in day_videos)
            day_followers = daily_followers_map.get(day, 0)
            for a in day_videos:
                a['followers'] = (day_followers * a['play_count']) / day_total_views if day_total_views else 0
                followers += (day_followers * a['play_count']) / day_total_views if day_total_views else 0
            daily_stats_collection.insert_many(day_videos)
    
    except Exception as error:
        result = handle_parser_error(lambda: None, error, parser_name, proceed)
        status = result['status']
        proceed = result['proceed']
        print(f"{parser_name}: Error during processing: {str(error)}")
    finally:
        if db:
            save_parser_history(db, parser_name, start_time, 'followers', followers, status)
            close_mongo_connection(db.client)  
            log_parser_finish(parser_name)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'tiktok_daily_stats',
    default_args=default_args,
    description='Recalculate TikTok daily stats and save to MongoDB',
    schedule_interval=None,
    start_date=days_ago(1),
)

recalculate_tiktok_daily_stats_task = PythonOperator(
    task_id='recalculate_tiktok_daily_stats',
    python_callable=recalculate_tiktok_daily_stats,
    provide_context=True,
    dag=dag,
)

recalculate_tiktok_daily_stats_task
