import pandas as pd

def flatten_analytics_dict(student_analytics_dict):
    return [{
                
                'course_id': cid,
                'user_id': uid,
                'missing': data.get('missing', 0),
                'late': data.get('late', 0),
                'page_views': data.get('page_views', 0),
                'participations': data.get('participations', 0)
            }
            for cid, users in student_analytics_dict.items()
            for uid, data in users.items()
            ]


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
        return pd.DataFrame(columns=['user_id', 'course_id', 'missing', 'late', 'page_views', 'participations'])
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
        'missing', 'late', 'page_views', 'participations'
    ]
    df = df[final_columns]

    return df

def coerce_ids_to_str(df, id_columns):
    for col in id_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df

def clean_df(df):
    df['current_score'] = df['current_score'].fillna(0)
    df['missing'] = df['missing'].fillna(0)
    df['late'] = df['late'].fillna(0)
    df['page_views'] = df['page_views'].fillna(0)
    df['participations'] = df['participations'].fillna(0)

    df = df[
        df['sortable_name'].notna() & df['sortable_name'].ne('') &
        ~df['sortable_name'].astype(str).str.contains('Student, Test', na=False) &
        df['sis_course_id'].notna() & df['sis_course_id'].ne('') &
        ~df['sis_course_id'].astype(str).str.contains('UNV-800', na=False)
    ]

    return df

def filter_courses(course_df, level):
    sis_course_id_list = list(set(course_df['sis_course_id'].tolist()))
    dfs = []

    for sis in sis_course_id_list:
        if sis is None:
            continue
        elif len(sis.split('-')) > 2:
            course_num = int(sis.split('-')[2])
            if course_num < 500 and level == 'undergrad':
                dfs.append(course_df[course_df['sis_course_id'] == sis])
            elif course_num >= 500 and level == 'grad':
                dfs.append(course_df[course_df['sis_course_id'] == sis])

    filtered_df = pd.concat(dfs)
    return filtered_df