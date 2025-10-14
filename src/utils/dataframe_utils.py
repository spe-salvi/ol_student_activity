import pandas as pd

def flatten_analytics_dict(student_analytics_dict):
    return [{
                'user_id': uid,
                'course_id': cid,
                'missing': data.get('missing', ''),
                'late': data.get('late', ''),
                'last_pageview': data.get('last_pageview', None),
                'last_participation': data.get('last_participation', None)
            }
            for uid, courses in student_analytics_dict.items()
            for cid, data in courses.items()]


def flatten_course_dict(course_dict):    
    return [{'course_id': course_id, **data}
            for course_id, data in course_dict.items()]

def flatten_enrollments_dict(enrollments_dict):
    return [
        {'user_id': uid, 'course_id': cid, 'current_score': score}
        for uid, courses in enrollments_dict.items() if uid
        for cid, score in courses.items() if cid
    ]

def flatten_student_dict(student_dict):
    return [{'user_id': user_id, **data}
           for user_id, data in student_dict.items()]

def build_analytics_df(student_analytics_dict):
    rows = flatten_analytics_dict(student_analytics_dict)
    if not rows:
        print("⚠️ Warning: No enrollments returned. Check API.")
        return pd.DataFrame(columns=['user_id', 'course_id', 'current_score'])
    return pd.DataFrame(rows)

def build_course_df(course_dict):
    rows = flatten_course_dict(course_dict)
    if not rows:
        print("⚠️ Warning: No enrollments returned. Check API.")
        return pd.DataFrame(columns=['user_id', 'course_id', 'current_score'])
    return pd.DataFrame(rows)

def build_enrollments_df(enrollments_dict):
    rows = flatten_enrollments_dict(enrollments_dict)
    if not rows:
        print("⚠️ Warning: No enrollments returned. Check API.")
        return pd.DataFrame(columns=['user_id', 'course_id', 'current_score'])
    return pd.DataFrame(rows)

def build_student_df(student_dict):
    rows = flatten_student_dict(student_dict)
    if not rows:
        print("⚠️ Warning: No enrollments returned. Check API.")
        return pd.DataFrame(columns=['user_id', 'course_id', 'current_score'])
    return pd.DataFrame(rows)

def merge_dfs(analytics, courses, enrollments, students):
    enrollments = coerce_ids_to_str(enrollments, ['user_id', 'course_id'])
    students = coerce_ids_to_str(students, ['user_id'])
    courses = coerce_ids_to_str(courses, ['course_id'])
    analytics = coerce_ids_to_str(analytics, ['user_id', 'course_id'])

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

def coerce_ids_to_str(df, id_columns):
    """Convert all specified ID columns in a dataframe to string."""
    for col in id_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df

def clean_df(df):
    df['current_score'] = df['current_score'].fillna(0)
    df['missing'] = df['missing'].fillna(0)
    df['late'] = df['late'].fillna(0)

    df['last_pageview'] = pd.to_datetime(df['last_pageview'], errors='coerce')
    df['last_participation'] = pd.to_datetime(df['last_participation'], errors='coerce')
    df['last_pageview'] = df['last_pageview'].fillna(pd.NaT)
    df['last_participation'] = df['last_participation'].fillna(pd.NaT)

    df = df[
        df['sortable_name'].notna() & df['sortable_name'].ne('') &
        df['sis_course_id'].notna() & df['sis_course_id'].ne('') &
        ~df['sis_course_id'].str.contains('UNV-800')
    ]

    return df