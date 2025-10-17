import time
from datetime import datetime
from config.config import *
from controller import controller
import pandas as pd

def main():
    start = time.perf_counter()
    
    undergrad_df = controller(LEVEL[0])
    grad_df = controller(LEVEL[1])

    current_time = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    REPORT_FILE_NAME = f"./reports/online_student_activity_{current_time}.xlsx"

    with pd.ExcelWriter(REPORT_FILE_NAME, engine='openpyxl') as writer:
        undergrad_df.to_excel(writer, sheet_name='UndergradActivity', index=False)
        grad_df.to_excel(writer, sheet_name='GradActivity', index=False)

    end = time.perf_counter()
    elapsed = (end - start) / 60

    print(f"Report saved to {REPORT_FILE_NAME}")
    print(f"Elapsed time: {elapsed:.2f} minutes")

if __name__ == "__main__":
    main()