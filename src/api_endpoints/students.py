import config.config as config
from utils.retry_request import retry_get
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def student_api(user_id):

    return retry_get(f'{config.API_URL}/users/{user_id}',{})


def student_endpoint(user):
    return {
        user.get('id'): {
            'sortable_name': user.get('sortable_name', ''),
            'sis_user_id': user.get('sis_user_id', ''),
            'email': user.get('email', ''),
        }
    }

def build_all_students(enrollments_dict):
    student_dict = {}

    def fetch_user(uid):
        data = student_api(uid) or []
        return student_endpoint(data)

    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_user, uid): uid for uid in enrollments_dict}

        for future in as_completed(futures):
            result = future.result()
            student_dict.update(result)

    return student_dict