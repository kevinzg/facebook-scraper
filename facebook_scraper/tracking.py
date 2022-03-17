import threading
from datetime import datetime
from typing import Optional, TypedDict


class EventData(TypedDict):
    start_url: Optional[str]
    url: Optional[str]
    exception: Optional[str]
    status_code: Optional[int]
    retries: Optional[int]
    posts: Optional[int]
    page_number: Optional[int]


class Tracking:
    @staticmethod
    def record_event(event_name: str, data: EventData, remark: str = ""):
        # can be monkey patched
        Tracking.dummy(event_name, data, remark)

    @staticmethod
    def dummy(event_name: str, data: EventData, remark: str = ""):
        thread_id, thread_name = threading.get_ident(), threading.currentThread().getName()
        job_id = f"{thread_id}|{thread_name}"
        event_time = datetime.utcnow()

        print("====================")
        print(job_id, event_name, event_time, data, remark)


def record_event(event_name: str, data: EventData, remark: str = ""):
    Tracking.record_event(event_name, data, remark)
