import ot
import pandas as pd

def run_job():
    with ot.SQLiteHandler('onetime.db') as s:
        s.export_db_to_excel(
            'C:/users/william.mcguinness/scratch/onetime_db.xlsx'
        )
        df = s.sql_to_df(
            query="""
                SELECT r.room_name, r.room_location, g.game_name,
                    lg.table_count, lg.waiting_count, lg.updated
                FROM live_games lg
                    LEFT JOIN rooms r
                        ON lg.room_id = r.room_id
                    LEFT JOIN games g
                        ON lg.game_id = g.game_id
            """
        )

    ot.df_to_xl(
        {'Sheet1': df},
        'C:/users/william.mcguinness/scratch/live_games.xlsx'
    )



if __name__ == '__main__':
    # pd.set_option('display.max_columns', None)
    run_job()