# common/schemas.py

from dataclasses import dataclass, field
from typing import Optional

@dataclass
class VideoStats:
    collectCount: Optional[int] = None
    commentCount: Optional[int] = None
    diggCount: Optional[int] = None
    playCount: Optional[int] = None
    shareCount: Optional[int] = None
    followers_count: Optional[int] = None

@dataclass
class Video:
    author: Optional[str] = None
    createTime: Optional[str] = None
    description: Optional[str] = None
    id: Optional[str] = None
    duration: Optional[int] = None
    video_url: Optional[str] = None
    img_url: Optional[str] = None
    stats: VideoStats = field(default_factory=VideoStats)

@dataclass
class Document:
    _id: Optional[str] = None
    recordCreated: Optional[str] = None
    tags: Optional[str] = None
    platform: Optional[str] = None
    video: Video = field(default_factory=Video)
