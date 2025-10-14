import config.config as config
from utils.retry_request import retry_get


def pageviews_api(course_id, user_id):
    url = f'{config.API_URL}/courses/{course_id}/analytics/users/{user_id}/activity'
    return retry_get(url,{})


def pageview_endpoint(pageviews, user_id, course_id, pageviews_dict=None):

    pageviews_dict = pageviews_dict or {}

    page_views = pageviews.get('page_views', [])
    participations = pageviews.get('participations', [])

    last_pageview = page_views[-1] if page_views else None
    last_participation = participations[-1].get('created_at') if participations else None

    pageviews_dict.setdefault(user_id, {})[course_id] = {
        'last_pageview': last_pageview,
        'last_participation': last_participation,
    }

    return pageviews_dict


def build_student_pageviews(course_ids):
    all_pageviews = {}
    for cid in course_ids:
        data = pageviews_api(cid)  # your API call per course
        for user_id, pv in data.items():
            all_pageviews = pageview_endpoint(pv, user_id, cid, all_pageviews)

    return all_pageviews