# common/common_functions.py

from datetime import datetime
from pymongo import MongoClient
import pendulum
from tikapi import TikAPI
from typing import Any, Dict
from dataclasses import asdict
from airflow.models import Variable
from common.schemas import Document, Video, VideoStats

import logging
logger = logging.getLogger("airflow")

def get_mongo_client() -> MongoClient:
    mongo_url = Variable.get("MONGO_URL")
    mongo_dbname = Variable.get("MONGO_DBNAME")
    
    print(f"mongo_dbname: {mongo_dbname}, type: {type(mongo_dbname)}")  
    
    if not isinstance(mongo_dbname, str) or not mongo_dbname:
        raise TypeError(f"Invalid mongo_dbname: {mongo_dbname}")
    
    client = MongoClient(mongo_url)
    db = client[mongo_dbname]
    return db

def get_tikapi_client() -> TikAPI:
    api_key = Variable.get("TIKAPI_KEY")
    auth_key = Variable.get("TIKAPI_AUTHKEY")
    api = TikAPI(api_key)
    return api.user(accountKey=auth_key)

def save_data_to_mongo(collection_name: str, data: Dict[str, Any], ts: str) -> None:
    db = get_mongo_client()
    db[collection_name].insert_one({
        "data": data,
        "recordCreated": ts
    })
    
def parse_datetime(datetime_str):
    if isinstance(datetime_str, str):
        return pendulum.parse(datetime_str)
    elif isinstance(datetime_str, datetime):
        return pendulum.instance(datetime_str)
    return None

def save_parser_history(db, parser_name, start_time, data_type, total_count, status):
    db.parser_history.insert_one({
        "parser_name": parser_name,
        "start_time": start_time,
        "end_time": datetime.utcnow(),
        "data_type": data_type,
        "total_count": total_count,
        "status": status
    })

def combine_videos_object(document: Dict[str, Any], platform: str) -> Dict[str, Any]:
    video = document.get("video", {})
    stats = video.get("stats", {})
    return asdict(Document(
        _id=document.get("_id"),
        recordCreated=document.get("recordCreated"),
        tags=document.get("tags"),
        platform=platform,
        video=Video(
            author=video.get("author", {}).get("nickname") if platform == "tiktok" else video.get("author_name") if platform == "instagram" else video.get("author") if platform == "youtube" else None,
            createTime=video.get("createTime") if platform in ["tiktok", "youtube", "instagram"] else None,
            description=video.get("desc") if platform in ["tiktok", "youtube", "instagram"] else None,
            id=video.get("id"),
            duration=video.get("music", {}).get("duration") if platform == "tiktok" else None,
            video_url=video.get("video", {}).get("playAddr") if platform == "tiktok" else video.get("cover") if platform == "instagram" else video.get("url") if platform == "youtube" else None,
            img_url=video.get("video", {}).get("cover") if platform == "tiktok" else video.get("image_url") if platform == "instagram" else video.get("cover") if platform == "youtube" else None,
            stats=VideoStats(
                collectCount=stats.get("collectCount") if platform == "tiktok" else None,
                commentCount=stats.get("commentCount") if platform == "tiktok" else video.get("comment_count") if platform == "instagram" else None,
                diggCount=stats.get("diggCount") if platform == "tiktok" else video.get("like_count") if platform == "instagram" else None,
                playCount=stats.get("playCount") if platform == "tiktok" else None,
                shareCount=stats.get("shareCount") if platform == "tiktok" else None,
                followers_count=None if platform == "tiktok" else video.get("profile_picture_url", {}).get("followers_count") if platform == "instagram" else None,
            )
        )
    ))

def close_mongo_connection(mongo_client):
    try:
        mongo_client.close()
    except Exception as error:
        print(f"Failed to close MongoDB connection: {error}")
        logger.error(f"Failed to close MongoDB connection: {error}")

def log_parser_start(parser_name):
    print(f"{parser_name}: Started: {pendulum.now().to_iso8601_string()}")
    logger.info(f"{parser_name}: Started: {pendulum.now().to_iso8601_string()}")

def log_parser_finish(parser_name):
    end_time = pendulum.now()
    print(f"{parser_name}: Finished: {end_time.to_iso8601_string()}")
    logger.info(f"{parser_name}: Finished: {end_time.to_iso8601_string()}")

error_retry_counters = {}

def handle_parser_error(error, parser_name, max_retries=3):
    status = 'success'
    short_message = ''
    proceed = True

    if is_rate_limit_error(error):
        status = 'Rate limit reached'
        short_message = 'Error: rate_limit'
        print('Rate-Limit reached. Terminating function.')
        proceed = False
    elif is_quota_exceeded_error(error):
        status = 'Quota exceeded'
        short_message = 'Error: quota_exceeded'
        print('Quota exceeded. Terminating function.')
        proceed = False
    elif is_forbidden_error(error):
        if is_comments_disabled_error(error):
            print('Comments are disabled for this video. Continuing function.')
            proceed = True
            status = 'success'
        else:
            if parser_name not in error_retry_counters:
                error_retry_counters[parser_name] = 0
            error_retry_counters[parser_name] += 1

            print(f"Attempt {error_retry_counters[parser_name]} for {parser_name}")

            if error_retry_counters[parser_name] > max_retries:
                status = 'Forbidden request'
                short_message = 'Error: forbidden'
                print('Forbidden request. Terminating function after multiple retries.')
                proceed = False
            else:
                print(f'Forbidden request attempt {error_retry_counters[parser_name]}. Retrying...')
                proceed = True
    elif is_invalid_grant_error(error):
        status = 'Invalid grant'
        short_message = 'Error: invalid_grant'
        print('Invalid grant. Terminating function.')
        proceed = False
    elif is_undefined_property_error(error):
        print('Cannot read properties of undefined. Continuing function.')
        proceed = True
    else:
        short_message = f'Error: {str(error)}'
        print(f'{parser_name}: Error: {str(error)}')
        proceed = False

    if proceed:
        if parser_name in error_retry_counters:
            del error_retry_counters[parser_name]

    return {'status': 'success' if proceed else short_message, 'proceed': proceed}


def is_rate_limit_error(error: Any) -> bool:
    return 'rate_limit' in str(error)

def is_forbidden_error(error: Any) -> bool:
    return 'FORBIDDEN' in str(error)

def is_invalid_grant_error(error: Any) -> bool:
    return 'invalid_grant' in str(error)

def is_undefined_property_error(error: Any) -> bool:
    return 'Cannot read properties of undefined' in str(error)

def is_quota_exceeded_error(error: Any) -> bool:
    return 'quota' in str(error)

def is_comments_disabled_error(error: Any) -> bool:
    return 'commentsDisabled' in str(error)

def log_exception_details(error, parser_name):
    logging.error(f"{parser_name}: Error: {error}")
    logging.error(f"Attributes and methods of the exception: {dir(error)}")
    
    if hasattr(error, '__dict__'):
        logging.error(f"Exception details: {error.__dict__}")
