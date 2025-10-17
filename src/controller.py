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


# def collect_student_analytics(enrollments_dict):
#     """
#     Build {user_id: {course_id: {student_summary + pageview}}} atomically and resiliently.
#     """
#     analytics_dict = {}

#     # All (user_id, course_id) pairs
#     user_course_pairs = [
#         (str(uid), str(cid))
#         for uid, courses in enrollments_dict.items()
#         for cid in courses.keys()
#     ]

#     def fetch_and_merge(uid, cid):
#         try:
#             raw_summaries = summaries.student_summary_api(cid) or {}
#             print(f"[DEBUG] First summary in list: {raw_summaries[0]}")
#             summary_nested = summaries.student_summary_endpoint(raw_summaries[0], cid)
#             # Extract the relevant entry for this uid/cid, if any
#             summary_entry = summary_nested.get(uid, {}).get(cid, {})

#             # Pageviews: pageviews_api expects both course & user
#             raw_pv = activity.pageviews_api(cid, uid) or {}
#             pv_nested = activity.pageview_endpoint(raw_pv, uid, cid)
#             pageview_entry = pv_nested.get(uid, {}).get(cid, {})

#             raw_summaries = summaries.student_summary_api(cid)
#             print(f"🔍 Summary API raw for course {cid}: {type(raw_summaries)} {str(raw_summaries)[:200]}")

#             raw_pv = activity.pageviews_api(cid, uid)
#             print(f"🔍 Pageview API raw for user {uid}, course {cid}: {type(raw_pv)} {str(raw_pv)[:200]}")


#             merged = {**(summary_entry or {}), **(pageview_entry or {})}
#             print(f"[DEBUG] UID {uid}, CID {cid}")
#             print(f"  Summary Data: {summary_entry}")
#             print(f"  Pageview Data: {pageview_entry}")


#             return uid, cid, merged

#         except:
#             return uid, cid, {}

#     # Threaded execution
#     with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
#         futures = [executor.submit(fetch_and_merge, uid, cid) for uid, cid in user_course_pairs]
#         for future in as_completed(futures):
#             uid, cid, data = future.result()
#             analytics_dict.setdefault(uid, {})[cid] = data

#     return analytics_dict














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

