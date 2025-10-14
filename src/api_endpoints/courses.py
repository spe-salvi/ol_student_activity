import config.config as config
from utils.retry_request import retry_get


def courses_api():
    params = {
        'search_term' : config.SEARCH_TERM,
        'enrollment_term_id' : config.TERM
    }
    url = f'{config.API_URL}{config.FUS_ACCOUNT}/courses'

    return retry_get(url, params)


def courses_endpoint(courses):
    return {
        str(course.get('id', '')): {
            'sis_course_id': course.get('sis_course_id', ''),
            'course_name': course.get('name', '')
        }
        for course in courses
    }



def course_api(course_id):
    url = f'{config.API_URL}/courses/{course_id}'
    return retry_get(url, {})