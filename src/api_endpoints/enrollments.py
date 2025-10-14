import config.config as config
from utils.retry_request import retry_get
import pandas as pd


def enrollments_api(course_id):
    params = {
        'role' : 'StudentEnrollment'
    }
    
    url = f'{config.API_URL}/courses/{course_id}/enrollments'

    return retry_get(url,params)


def enrollments_endpoint(enrollments):
    return {
        s['user_id']: {s['course_id']: s.get('grades', {}).get('current_score')}
        for s in enrollments
    }

