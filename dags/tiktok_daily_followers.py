import sys
import os
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import datetime
import pendulum
from pymongo import UpdateOne
from typing import Any, Dict
from common.common_functions import close_mongo_connection, log_parser_finish, save_parser_history, get_mongo_client, parse_datetime

def recalculate_tiktok_daily_followers(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'Tiktok Daily Followers'
    status = 'success'
    start_time = pendulum.now()
    total_followers = 0

    try:
        db = get_mongo_client()
        followers_stats_collection = db['tiktok_followers']
        daily_followers_collection = db['tiktok_daily_followers']
        posts_stats_collection = db['tiktok_daily_stats']

        if 'tiktok_daily_followers' in db.list_collection_names():
            daily_followers_collection.drop()

        followers_stats = list(followers_stats_collection.find().sort('recordCreated', 1))
        if not followers_stats:
            raise ValueError("No followers stats found in the collection")

        first_stat = followers_stats[0]
        previous_stat_num = first_stat['data']['follower_num']['value']
        previous_stat_date = parse_datetime(first_stat['recordCreated'])

        overall_accumulator = previous_stat_num
        days = []

        for stat in followers_stats[1:]:
            current_stat_num = stat['data']['follower_num']['value']
            current_stat_date = parse_datetime(stat['recordCreated'])

            duration = (current_stat_date - previous_stat_date).total_seconds()
            daily_followers = (current_stat_num - previous_stat_num) / duration

            days.append({
                '_id': current_stat_date.format('YYYY-MM-DD'),
                'followers': daily_followers * duration
            })

            previous_stat_num = current_stat_num
            previous_stat_date = current_stat_date
            overall_accumulator = current_stat_num

        post_days = list(posts_stats_collection.aggregate([
            {'$match': {'date': {'$lt': first_stat['recordCreated']}}},
            {'$group': {'_id': '$date', 'views': {'$sum': '$play_count'}}},
            {'$sort': {'_id': -1}}
        ]))

        views_to_distribute = sum(day['views'] for day in post_days)
        followers_by_views = overall_accumulator / views_to_distribute

        for day in post_days:
            followers = followers_by_views * day['views']
            days.insert(0, {
                '_id': day['_id'],
                'followers': followers,
            })
            total_followers += followers

        operations = [UpdateOne({'_id': day['_id']}, {'$set': day}, upsert=True) for day in days]
        daily_followers_collection.bulk_write(operations, ordered=False)

    except Exception as error:
        status = 'failure'
        print(f"Tiktok Daily Followers: Error during processing: {str(error)}")
        raise
    finally:
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
    'tiktok_daily_followers',
    default_args=default_args,
    description='Recalculate TikTok daily followers and save to MongoDB',
    schedule_interval=None,
    start_date=days_ago(1),
)

tiktok_followers_task = PythonOperator(
    task_id='tiktok_daily_followers',
    python_callable=recalculate_tiktok_daily_followers,
    provide_context=True,
    dag=dag,
)

tiktok_followers_task
