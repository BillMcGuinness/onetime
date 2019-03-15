import ot
import pandas as pd

def run_job():
    df_dict = {}
    with ot.SQLiteHandler('onetime.db') as s:
        all_tables = s.sql_to_df(
            query="""
                SELECT name 
                FROM sqlite_master 
                WHERE type='table' 
            """
        )
        all_tables = all_tables['name'].tolist()
        for table in all_tables:
            df = s.sql_to_df(
                query="""
                    SELECT *
                    FROM {}
                """.format(table)
            )
            df_dict[table] = df

    writer = pd.ExcelWriter(
        'C:/users/william.mcguinness/scratch/onetime_db.xlsx'
    )
    for table, df in df_dict.items():
        df.to_excel(writer, sheet_name=table)

    writer.save()


if __name__ == '__main__':
    # pd.set_option('display.max_columns', None)
    run_job()