import config.config as config
from utils.retry_request import retry_get
import pandas as pd

def student_summary_api(course_id):

    return (retry_get(f'{config.API_URL}/courses/{course_id}/analytics/student_summaries',{}))


def student_summary_endpoint(summaries, course_id, summary_dict=None):
    summary_dict = summary_dict or {}

    for summary in summaries:
        sid = summary.get('id')
        tardiness = summary.get('tardiness_breakdown', {}) or {}

        summary_dict.setdefault(sid, {})[course_id] = {
            'missing': tardiness.get('missing', ''),
            'late': tardiness.get('late', ''),
        }

    return summary_dict


def build_student_summaries(course_ids):
    summaries_by_student = {}

    for cid in course_ids:
        summaries = student_summary_api(cid)
        summaries_by_student = student_summary_endpoint(summaries, cid, summaries_by_student)

    return summaries_by_student
