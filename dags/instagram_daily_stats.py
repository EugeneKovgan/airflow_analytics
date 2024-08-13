import sys
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pendulum
from typing import Any, Dict, List
from common.common_functions import (
    close_mongo_connection,
    log_parser_finish,
    log_parser_start,
    save_parser_full_history,
    handle_parser_error,
    get_mongo_client,
)

# Coefficient calculation
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

# Difference calculation between stats
def diff_stats(stats: Dict, empty: Dict) -> Dict:
    diff = {}
    for k in empty.keys():
        if isinstance(stats.get(k, 0), (int, float)) and isinstance(empty.get(k, 0), (int, float)):
            diff[k] = stats.get(k, 0) - empty.get(k, 0)
        else:
            diff[k] = stats.get(k, 0)
    return diff

# Accumulate stats
def accumulate_stats(target: Dict, source: Dict):
    for key in target:
        if isinstance(target[key], (int, float)) and isinstance(source.get(key, 0), (int, float)):
            target[key] += source.get(key, 0)

# Multiply stats by a coefficient
def multiply_stats(s: Dict, multiplier: float) -> Dict:
    for key in s:
        if isinstance(s[key], (int, float)):
            s[key] *= multiplier
    return s

# Empty stats template
def empty_stats(time, post_id: str) -> Dict:
    platform = 'instagram'
    return {
        "postId": post_id,
        "date": time.format('YYYY-MM-DD'),
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
        "videoCreateTime":time,
        "total_duration": 0,
        "video_duration": 0,
        "followers": 0,
        "platform": platform,
    }

# Main function to calculate post statistics
def calculate_post(analytics_records: List[Dict], post_id: str) -> List[Dict]:
    now = pendulum.now()
    today = now.start_of('day')
    stats = analytics_records.pop(0) if analytics_records else None
    if not stats:
        return []

    time = pendulum.from_timestamp(stats['videoCreateTime'])

    previous = empty_stats(time, post_id)

    volume_before_the_first_record = diff_stats(stats, empty_stats(time, post_id))
    days_before_the_first_round = stats['recordCreated'].diff(time).in_days()

    def get_daily_speed(day):
        coeff = get_coefficient(day, days_before_the_first_round)
        return {
            "collect_count": volume_before_the_first_record['collect_count'] * coeff,
            "comment_count": volume_before_the_first_record['comment_count'] * coeff,
            "digg_count": volume_before_the_first_record['digg_count'] * coeff,
            "download_count": volume_before_the_first_record['download_count'] * coeff,
            "forward_count": volume_before_the_first_record['forward_count'] * coeff,
            "lose_comment_count": volume_before_the_first_record['lose_comment_count'] * coeff,
            "lose_count": volume_before_the_first_record['lose_count'] * coeff,
            "play_count": volume_before_the_first_record['play_count'] * coeff,
            "share_count": volume_before_the_first_record['share_count'] * coeff,
            "whatsapp_share_count": volume_before_the_first_record['whatsapp_share_count'] * coeff,
            "finish_rate": volume_before_the_first_record['finish_rate'],
            "total_duration": volume_before_the_first_record['total_duration'],
        }

    virtual = days_before_the_first_round > 3
    day = 1
    daily_analytics = []

    while time < today:
        day_stats = empty_stats(time, post_id)
        current_date = time.start_of('day')
        next_date = current_date.add(days=1)

        inside_previous = empty_stats(previous['recordCreated'], post_id)
        accumulate_stats(inside_previous, previous)

        while stats and pendulum.instance(stats['recordCreated']).in_tz('UTC') < next_date and analytics_records:
            diff = diff_stats(stats, inside_previous)
            accumulate_stats(day_stats, diff)
            if analytics_records:
                accumulate_stats(inside_previous, diff)
                stats = analytics_records.pop(0)
            virtual = False

        if virtual:
            proportion1 = multiply_stats(get_daily_speed(day - 1), day == 1 and time.diff(current_date).in_days())
            proportion2 = multiply_stats(get_daily_speed(day), next_date.diff(time).in_days())
            accumulate_stats(day_stats, proportion1)
            accumulate_stats(day_stats, proportion2)
        else:
            if stats and (next_date - pendulum.instance(stats['recordCreated']).in_tz('UTC')).in_hours() < 24:
                diff = diff_stats(stats, inside_previous)
                whole_duration = stats['recordCreated'].diff(inside_previous['recordCreated']).in_days()
                today_duration = next_date.diff(inside_previous['recordCreated']).in_days()
                
                if whole_duration != 0:
                    accumulate_stats(day_stats, multiply_stats(diff, today_duration / whole_duration))

        accumulate_stats(previous, day_stats)
        previous['recordCreated'] = next_date
        time = time.add(days=1)
        day += 1
        day_stats['date'] = current_date.format('YYYY-MM-DD')
        day_stats['postId'] = post_id

        video_create_time = stats['videoCreateTime']

        if isinstance(video_create_time, int):
            video_create_time_ms = video_create_time * 1000
        else:
            video_create_time_ms = int(pendulum.parse(video_create_time).timestamp() * 1000)

        day_stats['recordCreated'] = {
            "_i": video_create_time_ms,
            "_d": pendulum.now('UTC'),
            "_i_": int(pendulum.instance(stats['recordCreated']).timestamp() * 1000),
            "_d_": current_date,
        }  

        daily_analytics.append(day_stats)

    return daily_analytics

# Main DAG function
def recalculate_instagram_daily_stats(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'Instagram Daily Stats'
    status = 'success'
    start_time = pendulum.now()
    log_parser_start(parser_name)

    data = {
        'followers': 0,
        'comments': 0,
        'videos': 0
    }

    try:
        db = get_mongo_client()
        if not db:
            raise ValueError("Failed to connect to MongoDB")
        print("Successfully connected to MongoDB")

        posts_collection = db['instagram_reels']
        posts_stats_collection = db['instagram_reels_stats']
        daily_stats_collection = db['instagram_daily_stats']
        collections = db.list_collection_names()

        daily_followers_collection = db['instagram_daily_followers']
        daily_followers_stats = list(daily_followers_collection.find())
        daily_followers_map = {x['_id']: x['followers'] for x in daily_followers_stats}
        daily_views_map = {}

        posts = list(posts_collection.find({}))
        data['videos'] = len(posts)

        for p in posts:
            post_id = p['_id']
            video_create_time = p['video']['createTime']
            analytics_records = list(posts_stats_collection.find({'postId': post_id}).sort('recordCreated', 1))
            
            # Log each post before processing
            print(f"Processing post: {post_id}")
            # print(f"Analytics Records: {analytics_records}")

            for record in analytics_records:
                record['statistics']['recordCreated'] = pendulum.instance(record['recordCreated']).in_tz('UTC')
                record['statistics']['videoCreateTime'] = video_create_time
                record['statistics']['digg_count'] = record['statistics'].get('like_count', 0)
                record['statistics']['forward_count'] = 0
                record['statistics']['finish_rate'] = 0
                record['statistics']['total_duration'] = 0
                record['statistics']['download_count'] = 0
                record['statistics']['lose_comment_count'] = 0
                record['statistics']['lose_count'] = 0
                record['statistics']['whatsapp_share_count'] = 0

            analytics = calculate_post([r['statistics'] for r in analytics_records], post_id)
            if analytics:
                for a in analytics:
                    a['postId'] = post_id
                    if a['date'] not in daily_views_map:
                        daily_views_map[a['date']] = []
                    daily_views_map[a['date']].append(a)

        if 'instagram_daily_stats' in collections:
            daily_stats_collection.drop()

        for day in daily_views_map:
            day_videos = daily_views_map[day]
            if day in daily_followers_map:
                day_total_views = sum(x['play_count'] for x in day_videos)
                if day_total_views > 0:
                    day_followers = daily_followers_map[day]
                    for a in day_videos:
                        a['followers'] = (day_followers * a['play_count']) / day_total_views
                        data['followers'] += a['followers']
            if day_videos:
                daily_stats_collection.insert_many(day_videos)

    except Exception as error:
        status = handle_parser_error(error, parser_name)
        print(f"An error occurred: {error}")
    finally:
        if db:
            save_parser_full_history(db, parser_name, start_time, data, status)
            close_mongo_connection(db.client)
        log_parser_finish(parser_name)

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
dag = DAG(
    'instagram_daily_stats',
    default_args=default_args,
    description='Recalculate Instagram daily stats and save to MongoDB',
    schedule_interval='0 23 * * *',
    start_date=days_ago(1),
    catchup=False,
)

# Define the Python task in the DAG
recalculate_instagram_daily_stats_task = PythonOperator(
    task_id='recalculate_instagram_daily_stats',
    python_callable=recalculate_instagram_daily_stats,
    provide_context=True,
    dag=dag,
)

# Assign the task to the DAG
recalculate_instagram_daily_stats_task
