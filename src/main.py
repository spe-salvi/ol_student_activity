import time
from datetime import datetime
from config.config import *
from controller import controller

def main():
    start = time.perf_counter()
    
    results_df = controller()

    current_time = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    REPORT_FILE_NAME = f"./reports/online_student_activity_{current_time}.xlsx"

    results_df.to_excel(REPORT_FILE_NAME,sheet_name='StudentActivity')

    end = time.perf_counter()
    seconds = end - start
    elapsed = seconds / 60

    print(f'Elapsed time: {elapsed:.2f} minutes')
if __name__ == "__main__":
    main()