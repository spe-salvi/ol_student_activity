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
        str(c.get('id', '')): {
            'sis_course_id': c.get('sis_course_id', ''),
            'course_name': c.get('name', '')
        }
        for c in courses
    }

def build_all_courses():
    cs = courses_api() or []
    return courses_endpoint(cs)



def course_api(course_id):
    url = f'{config.API_URL}/courses/{course_id}'
    return retry_get(url, {})