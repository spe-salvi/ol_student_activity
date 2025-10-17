import config.config as config
from utils.retry_request import retry_get
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def pageviews_api(course_id, user_id):
    url = f'{config.API_URL}/courses/{course_id}/analytics/users/{user_id}/activity'
    return retry_get(url,{})


def pageview_endpoint(pageviews, user_id, course_id, pageviews_dict=None):
    pageviews_dict = pageviews_dict or {}

    if not isinstance(pageviews, dict) or (isinstance(pageviews, dict) and 'errors' in pageviews):
        return pageviews_dict
    
    if not isinstance(pageviews, list):
        return pageviews_dict
    
    page_views = pageviews.get('page_views') or []
    participations = pageviews.get('participations') or []

    last_pageview = page_views[-1] if page_views else None
    last_participation = None
    if participations:
        last = participations[-1]
        if isinstance(last, dict):
            last_participation = last.get('created_at')

    pageviews_dict.setdefault(str(user_id), {})[str(course_id)] = {
        'last_pageview': last_pageview,
        'last_participation': last_participation,
    }

    return pageviews_dict


# def build_student_pageviews(course_ids):
#     all_pageviews = {}

#     def fetch(cid):
#         data = pageviews_api(cid) or []
#         return cid, data

#     with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
#         futures = [executor.submit(fetch, cid) for cid in course_ids]

#         for future in as_completed(futures):
#             cid, data = future.result()
#             for uid, pv in data.items():
#                 all_pageviews = pageview_endpoint(pv, uid, cid, all_pageviews)

#     return all_pageviews