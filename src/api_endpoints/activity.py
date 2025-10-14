import config.config as config
from utils.retry_request import retry_get
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def pageviews_api(course_id, user_id):
    url = f'{config.API_URL}/courses/{course_id}/analytics/users/{user_id}/activity'
    return retry_get(url,{})


def pageview_endpoint(pageviews, user_id, course_id, pageviews_dict=None):
    pageviews_dict = pageviews_dict or {}

    if not isinstance(pageviews, dict):
        return pageviews_dict

    last_pageview = pageviews.get('page_views', [None])[-1]
    participations = pageviews.get('participations', [])
    last_participation = (
        participations[-1].get('created_at')
        if participations and isinstance(participations[-1], dict)
        else None
    )

    new_entry = {
        user_id: {course_id: {            
            'last_pageview': last_pageview,
            'last_participation': last_participation
        }}
    }

    for uid, data in new_entry.items():
        pageviews_dict.setdefault(uid, {}).update(data)

    return pageviews_dict


def build_student_pageviews(course_ids):
    all_pageviews = {}

    def fetch(cid):
        data = pageviews_api(cid) or []
        return cid, data

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(fetch, cid) for cid in course_ids]

        for future in as_completed(futures):
            cid, data = future.result()
            for uid, pv in data.items():
                all_pageviews = pageview_endpoint(pv, uid, cid, all_pageviews)

    return all_pageviews