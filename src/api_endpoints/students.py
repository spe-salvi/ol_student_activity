import config.config as config
from utils.retry_request import retry_get


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