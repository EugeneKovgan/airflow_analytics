import sys
sys.path.append('/mnt/e/Symfa/airflow_analytics')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from common.common_functions import close_mongo_connection, get_mongo_client, handle_parser_error, log_parser_finish, log_parser_start, save_parser_history
import pendulum

def recalculate_tiktok_daily_followers_combine():
    parser_name = 'Tiktok Daily Followers Combine'
    status = 'success'
    platform = 'tiktok'
    start_time = pendulum.now()
    log_parser_start(parser_name)

    db = get_mongo_client()
    total_followers = 0

    try:       
        followers_stats_collection = db['followers']
        daily_followers_collection = db['tiktok_daily_followers']
        posts_stats_collection = db['tiktok_daily_stats']
        collection_names = db.list_collection_names()

        if 'daily_followers' in collection_names:
            daily_followers_collection.delete_many({"platform": platform})

        followers_stats = list(followers_stats_collection.find({"platform": platform}).sort('recordCreated', 1))
        if not followers_stats:
            raise ValueError("No followers stats found for the platform.")

        first_record = followers_stats[0]
        first_num = first_record['data']['follower_num']['value']
        first_date = pendulum.parse(str(first_record['recordCreated']))  

        previous_stat_num = first_num
        previous_stat_date = first_date

        today = pendulum.now().start_of('day')
        days = []
        overall_accumulator = first_num

        for date in pendulum.period(first_date.start_of('day'), today).range('days'):
            next_day = date.add(days=1)
            day_accumulator = 0

            current_stat = next((stat for stat in followers_stats if pendulum.parse(str(stat['recordCreated'])).is_same_day(date)), None)

            if current_stat:
                current_stat_num = current_stat['data']['follower_num']['value']
                day_accumulator = current_stat_num - overall_accumulator
                overall_accumulator = current_stat_num
                previous_stat_date = pendulum.parse(str(current_stat['recordCreated']))
                previous_stat_num = current_stat_num
            else:
                if previous_stat_date != date:
                    speed = (previous_stat_num - overall_accumulator) / previous_stat_date.diff(date).in_seconds()
                    if speed > 0:
                        end_of_day_reminder = speed * next_day.diff(previous_stat_date).in_seconds()
                        day_accumulator += end_of_day_reminder
                        overall_accumulator += end_of_day_reminder

            days.append({
                '_id': date.to_date_string(),
                'followers': day_accumulator,
                'platform': platform,
            })

            total_followers += day_accumulator
            print(f"Date: {date.to_date_string()}, Followers: {day_accumulator}")

        post_days = list(posts_stats_collection.aggregate([
            {'$match': {'date': {'$lt': first_date.to_date_string()}}},
            {'$group': {'_id': '$date', 'views': {'$sum': '$play_count'}}},
            {'$sort': {'_id': -1}},
        ]))

        views_to_distribute = sum(x['views'] for x in post_days)
        followers_by_views = first_num / views_to_distribute if views_to_distribute else 0

        for day in post_days:
            followers_for_day = followers_by_views * day['views']
            days.insert(0, {
                '_id': day['_id'],
                'followers': followers_for_day,
                'platform': platform, 
            })
            total_followers += followers_for_day

        daily_followers_collection.insert_many(days)
    except Exception as error:
        status = handle_parser_error(error, parser_name)
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
    'tiktok_daily_followers_combine',
    default_args=default_args,
    description='Recalculate TikTok daily followers and save to MongoDB',
    schedule_interval='25 3,7,11,15,19 * * *',  # Cron expression for scheduling
    start_date=days_ago(1),
    catchup=False,
) 

tiktok_daily_followers_combine_task = PythonOperator(
    task_id='recalculate_tiktok_daily_followers_combine',
    python_callable=recalculate_tiktok_daily_followers_combine,
    provide_context=True,
    dag=dag,
)

tiktok_daily_followers_combine_task
