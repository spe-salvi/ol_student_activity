import pandas as pd

def flatten_analytics_dict(analytics_dict):
    return [{
                'user_id': uid,
                'course_id': cid,
                # flatten student_summary
                **data.get('student_summary', {}),
                # flatten pageview
                **data.get('pageview', {})
            }
            for uid, courses in analytics_dict.items()
            for cid, data in courses.items()]


def flatten_course_dict(course_dict):    
    return [{'course_id': course_id, **data}
            for course_id, data in course_dict.items()]

def flatten_enrollments_dict(enrollments_dict):
    return [{'user_id': uid, 'course_id': cid, 'current_score': score}
        for uid, courses in enrollments_dict.items()
        for cid, score in courses.items()]

def flatten_student_dict(student_dict):
    return [{'user_id': user_id, **data}
           for user_id, data in student_dict.items()]

def build_analytics_df(student_analytics_dict):
    rows = flatten_analytics_dict(student_analytics_dict)
    return pd.DataFrame(rows)

def build_course_df(course_dict):
    rows = flatten_course_dict(course_dict)
    return pd.DataFrame(rows)

def build_enrollments_df(enrollments_dict):
    rows = flatten_enrollments_dict(enrollments_dict)
    return pd.DataFrame(rows)

def build_student_df(student_dict):
    rows = flatten_student_dict(student_dict)
    return pd.DataFrame(rows)

def merge_dfs(analytics, courses, enrollments, students):
    df = enrollments.copy()

    df = df.merge(students, on='user_id', how='left')

    df = df.merge(courses, on='course_id', how='left')

    df = df.merge(analytics, on=['user_id', 'course_id'], how='left')

    final_columns = [
        'sortable_name', 'sis_user_id', 'email', 
        'sis_course_id', 'course_name', 'current_score',
        'missing', 'late', 'last_pageview', 'last_participation'
    ]
    df = df[final_columns]

    return df

def clean_df(df):
    df['current_score'] = df['current_score'].fillna(0)
    df['missing'] = df['missing'].fillna(0)
    df['late'] = df['late'].fillna(0)
    df['last_pageview'] = df['last_pageview'].fillna(None)
    df['last_participation'] = df['last_participation'].fillna(None)

    df = df[
        df['sortable_name'].notna() & df['sortable_name'].ne('') &
        df['sis_course_id'].notna() & df['sis_course_id'].ne('') &
        ~df['sis_course_id'].str.contains('UNV-800')
    ]

    return df