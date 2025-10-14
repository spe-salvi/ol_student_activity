import config.config as config
from utils.retry_request import retry_get
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def enrollments_api(course_id):
    params = {
        'role' : 'StudentEnrollment'
    }
    
    url = f'{config.API_URL}/courses/{course_id}/enrollments'

    return retry_get(url,params)


def enrollments_endpoint(enrollments):
    return {
        s.get('user_id'): {
            s.get('course_id'): s.get('grades', {}).get('current_score', None)
        }
        for s in enrollments
        if s.get('user_id') and s.get('course_id')
    }


def build_all_enrollments(course_dict):
    all_enrollments = {}

    def fetch_and_process(cid):
        es = enrollments_api(cid) or []
        return enrollments_endpoint(es)

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_and_process, cid): cid for cid in course_dict}

        for future in as_completed(futures):
            enroll_dict = future.result()
            for uid, courses in enroll_dict.items():
                all_enrollments.setdefault(uid, {}).update(courses)

    return all_enrollments

