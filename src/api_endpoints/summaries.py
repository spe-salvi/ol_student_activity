import config.config as config
from utils.retry_request import retry_get
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


def student_summary_api(course_id):

    return (retry_get(f'{config.API_URL}/courses/{course_id}/analytics/student_summaries',{}))


def student_summary_endpoint(summaries, course_id, summary_dict=None):
    summary_dict = summary_dict or {}

    if not isinstance(summaries, dict):
        return summary_dict

    new_entries = {
        s['id']: {course_id: {
            'missing': s.get('tardiness_breakdown', {}).get('missing', ''),
            'late': s.get('tardiness_breakdown', {}).get('late', '')
        }}
        for s in summaries if isinstance(s, dict) and 'id' in s
    }

    for sid, data in new_entries.items():
        summary_dict.setdefault(sid, {}).update(data)

    return summary_dict


def build_student_summaries(course_ids):
    summaries_by_student = {}

    def fetch(cid):
        summaries = student_summary_api(cid) or []
        return cid, student_summary_endpoint(summaries, cid)

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = [executor.submit(fetch, cid) for cid in course_ids]

        for future in as_completed(futures):
            cid, result = future.result()
            for uid, data in result.items():
                summaries_by_student.setdefault(uid, {}).update(data)

    return summaries_by_student
