import config.config as config
from utils.retry_request import retry_get
import pandas as pd
from tqdm import tqdm
from time import perf_counter
import datetime
import api_endpoints.courses as courses
import api_endpoints.enrollments as enrollments
import api_endpoints.students as students
import api_endpoints.activity as activity
import api_endpoints.summaries as summaries
from utils.dataframe_utils import *
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


def controller(level):
    course_dict = courses.build_all_courses()
    enrollments_dict = enrollments.build_all_enrollments(course_dict)
    student_analytics_dict = summaries.build_student_summaries(course_dict)
    student_dict = students.build_all_students(enrollments_dict)

    print("=== ANALYTICS DICT SNAPSHOT ===")
    import json
    print(json.dumps({k: list(v.keys()) for k, v in list(student_analytics_dict.items())[:5]}, indent=2))
    print("Example nested data:")
    for cid, user in list(student_analytics_dict.items())[:3]:
        for uid, data in user.items():
            print(f"User {uid}, Course {cid}, Data: {data}")
            break


    course_df = build_course_df(course_dict)
    course_df = filter_courses(course_df, level)
    enrollments_df = build_enrollments_df(enrollments_dict)
    analytics_df = build_analytics_df(student_analytics_dict)
    student_df = build_student_df(student_dict)
    final_df = merge_dfs(analytics_df, course_df, enrollments_df, student_df)
    return clean_df(final_df)
