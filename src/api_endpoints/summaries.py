import config.config as config
from utils.retry_request import retry_get
from concurrent.futures import ThreadPoolExecutor, as_completed


def student_summary_api(course_id):

    return (retry_get(f'{config.API_URL}/courses/{course_id}/analytics/student_summaries',{}))


def student_summary_endpoint(summaries, course_id, summary_dict=None):
    summary_dict = summary_dict or {}
    print(f'Sum_dict: {summary_dict}')

    if 'errors' in summaries:
        return summary_dict
    
    if not isinstance(summaries, list):
        return summary_dict

    print(f'Summaries: {summaries}')
    new_entries = {
        str(s['id']): {
            'missing': s.get('tardiness_breakdown', {}).get('missing', 0),
            'late': s.get('tardiness_breakdown', {}).get('late', 0),
            'page_views': s.get('page_views', 0),
            'participations': s.get('participations', 0)
        }
        for s in summaries if isinstance(s, dict) and 'id' in s
    }

    summary_dict.setdefault(str(course_id), {}).update(new_entries)

    return summary_dict


def build_student_summaries(course_dict):
    summaries_by_course = {}

    def fetch(cid):
        summaries = student_summary_api(cid) or []
        return cid, student_summary_endpoint(summaries, cid)

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [
            executor.submit(fetch, str(cid)) for cid in course_dict.keys()
        ]

        for future in as_completed(futures):
            cid, result = future.result()
            summaries_by_course.setdefault(str(cid), {}).update(result.get(str(cid), {}))

    return summaries_by_course


