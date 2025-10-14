import config.config as config
from utils.retry_request import retry_get
import pandas as pd
from tqdm import tqdm
from time import perf_counter
import datetime
import api_endpoints.courses as courses
import api_endpoints.enrollments as enrollments
import api_endpoints.students as students
from api_endpoints.activity import build_student_pageviews
from api_endpoints.summaries import build_student_summaries
from utils.dataframe_utils import *


def controller():
    cs = courses.courses_api()
    course_dict = courses.courses_endpoint(cs)

    student_analytics_dict = collect_student_analytics(course_dict)
    enrollments_dict = collect_all_enrollments(course_dict)
    student_dict = collect_all_students(enrollments_dict)

    analytics_df = build_analytics_df(student_analytics_dict)
    course_df = build_course_df(course_dict)
    enrollments_df = build_enrollments_df(enrollments_dict)
    student_df = build_student_df(student_dict)
    final_df = merge_dfs(analytics_df, course_df, enrollments_df, student_df)
    return clean_df(final_df)


def collect_student_analytics(course_dict):
    course_ids = list(course_dict.keys())

    summaries = build_student_summaries(course_ids)
    pageviews = build_student_pageviews(course_ids)

    return {
        uid: {
            cid: {
                **summaries.get(uid, {}).get(cid, {}),
                **pageviews.get(uid, {}).get(cid, {})
            }
            for cid in set(summaries.get(uid, {})) | set(pageviews.get(uid, {}))
        }
        for uid in set(summaries) | set(pageviews)
    }


def collect_all_enrollments(course_dict):
    all_enrollments = {}

    for cid in course_dict:
        es = enrollments.enrollments_api(cid)
        enroll_dict = enrollments.enrollments_endpoint(es)

        for uid, courses in enroll_dict.items():
            if uid not in all_enrollments:
                all_enrollments[uid] = {}
            all_enrollments[uid].update(courses)

    return all_enrollments

def collect_all_students(enrollments_dict):
    student_dict = {}

    for user_id in enrollments_dict:
        user_data = students.student_api(user_id)
        user_dict = students.student_endpoint(user_data)
        student_dict.update(user_dict)

    return student_dict








# def student_df(enrollment_df):
#     user_ids = []
#     names = []
#     sis_user_ids = []
#     emails = []

#     for student in tqdm(range(enrollment_df.shape[0])):
#         user_id = enrollment_df.iat[student,1]
#         user_ids.append(user_id)
#         try:
#             gsd = get_student_data(user_id)
#             names.append(str(gsd[0]))
#             sis_user_ids.append(str(gsd[1]))
#             emails.append(str(gsd[2]))
#         except:
#             names.append('---')
#             sis_user_ids.append('---')
#             emails.append('---')

#     sdf = pd.DataFrame()
#     sdf['User ID'] = user_ids
#     sdf['Name'] = names
#     sdf['SIS User ID'] = sis_user_ids
#     sdf['Email'] = emails
#     return sdf

# def controller():

#     courseids = online_courses()
#     # {user_id : course_id}
#     enrollments_list = enrollments(courseids)
#     id_dict = enrollments_list[1]
#     # [course_id,user_id,score]
#     enrollment_df = enrollments_list[0]
#     enrollment_df.drop_duplicates(inplace=True)
#     enrollment_df.to_excel('enrollments.xlsx',sheet_name='Enrollments')
#     # [user_id,name,sis_user_id]
#     student_df = student_df(enrollment_df)
#     student_df.drop_duplicates(inplace=True)
#     # [course_id,course_sis_id,user_id,missing,late]
#     late_missing_df = late_missing(courseids)
#     late_missing_df.drop_duplicates(inplace=True)
#     # [user_id,last page view,last part]
#     page_view_df = pageview(id_dict)
#     page_view_df.drop_duplicates(inplace=True)

#     # Merge the enrollment and student activity dataframes on the userid key
#     sis_id_df = pd.merge(enrollment_df,late_missing_df,how='inner',on=['Course ID','User ID'])
#     sis_id_df.drop_duplicates(inplace=True)

#     prime = pd.merge(student_df,sis_id_df,how='left',left_on=['User ID'],right_on=['User ID'])

#     # # # Merge the prime dataframe with the pageview dataframe on the userid key
#     final = pd.merge(prime,page_view_df,how='right',left_on=['User ID'],right_on=['User ID'])
#     final.drop(final.filter(items=['User ID_x']).columns,axis=1,inplace=True)
#     final.drop(final.filter(items=['User ID_y']).columns,axis=1,inplace=True)
#     final.drop(final.filter(items=['Course ID_x']).columns,axis=1,inplace=True)
#     final.drop(final.filter(items=['Course ID_y']).columns,axis=1,inplace=True)
#     final.drop_duplicates(inplace=True)

#     return final

