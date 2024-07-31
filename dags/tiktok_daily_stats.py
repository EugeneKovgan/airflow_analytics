from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pendulum
from datetime import datetime
from typing import Any, Dict
from common.common_functions import (
    close_mongo_connection,
    log_parser_finish,
    log_parser_start,
    save_parser_history,
    handle_parser_error,
    get_mongo_client,
)

def get_coefficient(day: int, days_before_first_round: int) -> float:
    if day == 0:
        return 0
    elif day == 1:
        return 0.4
    elif day == 2:
        return 0.25
    elif day == 3:
        return 0.1
    elif day > 3:
        return 0.25 / (days_before_first_round - 3)
    return 0

def diff_stats(stats, empty):
    diff = {}
    for k in empty.keys():
        if isinstance(stats.get(k, 0), (int, float)) and isinstance(empty.get(k, 0), (int, float)):
            diff[k] = stats.get(k, 0) - empty.get(k, 0)
        else:
            diff[k] = stats.get(k, 0)
    return diff

def accumulate_stats(target, source):
    for key in target:
        if isinstance(target[key], (int, float)) and isinstance(source.get(key, 0), (int, float)):
            target[key] += source.get(key, 0)

def empty_stats(time):
    return {
        "collect_count": 0,
        "comment_count": 0,
        "digg_count": 0,
        "download_count": 0,
        "forward_count": 0,
        "lose_comment_count": 0,
        "lose_count": 0,
        "play_count": 0,
        "share_count": 0,
        "whatsapp_share_count": 0,
        "finish_rate": 0,
        "recordCreated": time,
        "total_duration": 0,
        "video_duration": 0,
        "followers": 0,
    }

def calculate_post(analytics_records, post_id, video_create_time):
    now = pendulum.now()
    today = now.start_of('day')
    stats = analytics_records.pop(0) if analytics_records else None
    if not stats:
        return []

    time = pendulum.instance(datetime.fromtimestamp(video_create_time))
    previous = empty_stats(time)

    volume_before_first_record = diff_stats(stats, empty_stats(time))
    days_before_first_round = (pendulum.instance(stats['recordCreated']) - time).days

    def get_daily_speed(day):
        coeff = get_coefficient(day, days_before_first_round)
        return {k: v * coeff for k, v in volume_before_first_record.items() if isinstance(v, (int, float))}

    virtual = days_before_first_round > 3
    day = 1

    daily_analytics = []
    while time < today:
        day_stats = empty_stats(time)
        current_date = time.start_of('day')
        next_date = current_date.add(days=1)

        inside_previous = empty_stats(previous['recordCreated'])
        accumulate_stats(inside_previous, previous)

        while stats and pendulum.instance(stats['recordCreated']) < next_date and analytics_records:
            diff = diff_stats(stats, inside_previous)
            accumulate_stats(day_stats, diff)
            if analytics_records:
                accumulate_stats(inside_previous, diff)
                stats = analytics_records.pop(0)
            virtual = False

        if virtual:
            proportion1 = get_daily_speed(day - 1)
            proportion2 = get_daily_speed(day)
            accumulate_stats(day_stats, proportion1)
            accumulate_stats(day_stats, proportion2)
        else:
            if stats and (next_date - pendulum.instance(stats['recordCreated'])).days < 24:
                diff = diff_stats(stats, inside_previous)
                whole_duration = (pendulum.instance(stats['recordCreated']) - inside_previous['recordCreated']).days
                today_duration = (next_date - inside_previous['recordCreated']).days
                if whole_duration > 0:
                    accumulate_stats(day_stats, {k: v * (today_duration / whole_duration) for k, v in diff.items() if isinstance(v, (int, float))})

        accumulate_stats(previous, day_stats)
        previous['recordCreated'] = next_date
        time = time.add(days=1)
        day += 1

        # Добавление даты в day_stats
        day_stats['date'] = current_date.format('YYYY-MM-DD')
        
        daily_analytics.append(day_stats)

    return daily_analytics

def recalculate_tiktok_daily_stats(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'Tiktok Daily Stats'
    status = 'success'
    start_time = pendulum.now()
    log_parser_start(parser_name)

    try:
        db = get_mongo_client()
        if not db:
            raise ValueError("Failed to connect to MongoDB")
        print("Successfully connected to MongoDB")

        total_followers = 0

        posts_collection = db['tiktok_posts']
        posts_stats_collection = db['tiktok_posts_stats']
        daily_stats_collection = db['tiktok_daily_stats']
        daily_followers_collection = db['tiktok_daily_followers']

        daily_followers_stats = list(daily_followers_collection.find())
        daily_followers_map = {x['_id']: x['followers'] for x in daily_followers_stats}
        daily_views_map = {}

        posts = list(posts_collection.find({}))

        batch_size = 10
        for i in range(0, len(posts), batch_size):
            batch = posts[i:i + batch_size]
            for p in batch:
                video_duration = p['video']['video']['duration']
                video_desc = p['video']['desc']
                print(f"Processing video {video_desc} (# {p['_id']}) (video_duration - {video_duration})")

                analytics_records = list(posts_stats_collection.find({'postId': p['_id']}).sort('recordCreated', 1))
                filtered_analytics_records = [x for x in analytics_records if 'video_info' in x['analytics']]

                analytics = calculate_post(
                    [{
                        'recordCreated': pendulum.instance(x['recordCreated']),
                        'videoCreateTime': x['analytics']['video_info']['create_time'],
                        'collect_count': x['analytics']['video_info']['statistics'].get('collect_count', 0),
                        'comment_count': x['analytics']['video_info']['statistics'].get('comment_count', 0),
                        'digg_count': x['analytics']['video_info']['statistics'].get('digg_count', 0),
                        'download_count': x['analytics']['video_info']['statistics'].get('download_count', 0),
                        'forward_count': x['analytics']['video_info']['statistics'].get('forward_count', 0),
                        'lose_comment_count': x['analytics']['video_info']['statistics'].get('lose_comment_count', 0),
                        'lose_count': x['analytics']['video_info']['statistics'].get('lose_count', 0),
                        'play_count': x['analytics']['video_info']['statistics'].get('play_count', 0),
                        'share_count': x['analytics']['video_info']['statistics'].get('share_count', 0),
                        'whatsapp_share_count': x['analytics']['video_info']['statistics'].get('whatsapp_share_count', 0),
                        'finish_rate': x['analytics'].get('finish_rate', {}).get('value', 0),
                        'total_duration': x['analytics'].get('video_total_duration', {}).get('value', 0)
                    } for x in filtered_analytics_records],
                    str(p['_id']),
                    p['video']['createTime'] 
                )

                if analytics and len(analytics) > 0:
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
            for video in day_videos:
                video['followers'] = (day_followers * video['play_count']) / day_total_views if day_total_views else 0
                total_followers += video['followers']
            if day_videos:
                daily_stats_collection.insert_many(day_videos)
                print(f"Data for {day} successfully saved to database.")
            else:
                print(f"No data to insert for {day}")

    except Exception as error:
        status = handle_parser_error(error, parser_name)
        print(f"An error occurred: {error}")
        raise
    finally:
        if db:
            save_parser_history(db, parser_name, start_time, 'followers', total_followers, status)
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
    schedule_interval='0 22 * * *',  # Cron expression for scheduling
    start_date=days_ago(1),
    catchup=False,
)

recalculate_tiktok_daily_stats_task = PythonOperator(
    task_id='recalculate_tiktok_daily_stats',
    python_callable=recalculate_tiktok_daily_stats,
    provide_context=True,
    dag=dag,
)

recalculate_tiktok_daily_stats_task
