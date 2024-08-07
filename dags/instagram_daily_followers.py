import sys
import os
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
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

def recalculate_instagram_daily_followers(**kwargs: Dict[str, Any]) -> None:
    parser_name = 'Instagram Daily Followers'
    status = 'success'
    platform = 'instagram'
    start_time = pendulum.now()
    log_parser_start(parser_name)

    db = get_mongo_client()
    total_followers = 0
    history_saved = False

    try:
        followers_stats_collection = db['instagram_followers']
        daily_followers_collection = db['instagram_daily_followers']
        posts_stats_collection = db['instagram_daily_stats']
        collections = db.list_collection_names()

        if 'instagram_daily_followers' in collections:
            daily_followers_collection.drop()

        followers_stats = list(followers_stats_collection.find({}).sort('recordCreated', 1))
        if not followers_stats:
            raise ValueError("No follower stats found")

        first_num = followers_stats[0]['data']['followers_count']
        first_date = followers_stats[0]['recordCreated']

        previous_stat_num = first_num
        previous_stat_date = pendulum.parse(first_date) if isinstance(first_date, str) else pendulum.instance(first_date)

        today = pendulum.today()
        overall_accumulator = 0

        days = []

        first_tracking_followers_count = 0

        i = 1
        date = previous_stat_date.start_of('day')

        while date < today and i < len(followers_stats):
            next_day = date.add(days=1)
            day_accumulator = 0

            current_stat_num = followers_stats[i]['data']['followers_count']
            current_stat_date = pendulum.parse(followers_stats[i]['recordCreated']) if isinstance(followers_stats[i]['recordCreated'], str) else pendulum.instance(followers_stats[i]['recordCreated'])

            if overall_accumulator == 0:
                delta_seconds = (current_stat_date - previous_stat_date).total_seconds()
                if delta_seconds == 0:
                    raise ValueError("Zero division error: current_stat_date and previous_stat_date are the same.")
                speed = (current_stat_num - previous_stat_num) / delta_seconds
                overall_accumulator = current_stat_num - (current_stat_date - date).total_seconds() * speed
                first_tracking_followers_count = overall_accumulator

            while date.start_of('day') == current_stat_date.start_of('day') and i < len(followers_stats):
                current_stat_num = followers_stats[i]['data']['followers_count']
                current_stat_date = pendulum.parse(followers_stats[i]['recordCreated']) if isinstance(followers_stats[i]['recordCreated'], str) else pendulum.instance(followers_stats[i]['recordCreated'])

                day_accumulator += current_stat_num - overall_accumulator
                overall_accumulator = current_stat_num

                previous_stat_date = current_stat_date
                previous_stat_num = current_stat_num

                i += 1

            delta_seconds = (current_stat_date - previous_stat_date).total_seconds()
            if delta_seconds != 0:
                speed = (current_stat_num - previous_stat_num) / delta_seconds
                if speed > 0:
                    end_of_day_reminder = speed * (next_day - previous_stat_date).total_seconds()
                    day_accumulator += end_of_day_reminder
                    overall_accumulator += end_of_day_reminder

            previous_stat_date = next_day
            previous_stat_num = overall_accumulator

            days.append({
                '_id': date.format('YYYY-MM-DD'),
                'followers': day_accumulator,
            })
            total_followers += day_accumulator

            date = next_day

        post_days = list(posts_stats_collection.aggregate([
            { '$match': { 'date': { '$lt': previous_stat_date.format('YYYY-MM-DD') } } },
            {
                '$group': {
                    '_id': '$date',
                    'views': { '$sum': '$play_count' },
                },
            },
            {
                '$sort': { "_id": -1 },
            },
        ]))

        views_to_distribute = sum(day['views'] for day in post_days)
        followers_by_views = first_tracking_followers_count / views_to_distribute if views_to_distribute else 0

        for day in post_days:
            followers = followers_by_views * day['views']
            days.insert(0, {
                '_id': day['_id'],
                'platform': platform,
                'followers': followers,
            })
            total_followers += followers

        if not days:
            raise ValueError("No days data to insert")

        daily_followers_collection.insert_many(days)
    except Exception as error:
        status = handle_parser_error(error, parser_name)
    finally:
        if db:
            if not history_saved:
                save_parser_history(db, parser_name, start_time, 'followers', total_followers, status)
                history_saved = True
        close_mongo_connection(db.client)
        log_parser_finish(parser_name)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'instagram_daily_followers',
    default_args=default_args,
    description='Recalculate Instagram daily followers',
    schedule_interval='25 8,15,21 * * *',  # Cron expression for scheduling
    start_date=days_ago(1),
    catchup=False, 
)

recalculate_instagram_daily_followers_task = PythonOperator(
    task_id='recalculate_instagram_daily_followers',
    python_callable=recalculate_instagram_daily_followers,
    provide_context=True,
    dag=dag,
)

recalculate_instagram_daily_followers_task
