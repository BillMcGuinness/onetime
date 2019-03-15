import ot
import pandas as pd

def run_job():
    with ot.SQLiteHandler('onetime.db') as s:
        s.export_db_to_excel(
            'C:/users/william.mcguinness/scratch/onetime_db.xlsx'
        )



if __name__ == '__main__':
    # pd.set_option('display.max_columns', None)
    run_job()