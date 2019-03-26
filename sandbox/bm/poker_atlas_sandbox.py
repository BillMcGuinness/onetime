import ot
import pandas as pd

def run_job():
    with ot.SQLiteHandler('onetime.db') as s:
        s.export_db_to_excel(
            'C:/users/william.mcguinness/scratch/onetime_db.xlsx'
        )
        room_data = s.sql_to_df(
            query="""
            SELECT r.room_name || ' (' || r.room_location || ')' as 'room',
                DATETIME(lg.updated) as 'updated',
                SUM(CASE
                    WHEN lg.waiting_count > 0
                        THEN ((9*lg.table_count) + (.75*lg.waiting_count))
                    WHEN lg.table_count > 0
                        THEN ((9*(lg.table_count-1)) + 5)
                    ELSE 0
                    END) AS 'member_count',
                SUM(lg.table_count) as 'total_table_count'
            FROM live_games lg
                LEFT JOIN rooms r
                    ON lg.room_id = r.room_id
                LEFT JOIN games g
                    ON lg.game_id = g.game_id
            GROUP BY r.room_name || ' (' || r.room_location || ')',
                DATETIME(lg.updated)
            """
        )
        room_game_data = s.sql_to_df(
            query="""
            SELECT r.room_name || ' (' || r.room_location || ')' as 'room',
                g.game_name,  
                DATETIME(lg.updated) as 'updated',
                CASE
                    WHEN lg.waiting_count > 0
                        THEN ((9*lg.table_count) + (.75*lg.waiting_count))
                    WHEN lg.table_count > 0
                        THEN ((9*(lg.table_count-1)) + 5)
                    ELSE 0
                    END AS 'member_count',
                lg.table_count
                FROM live_games lg
                    LEFT JOIN rooms r
                        ON lg.room_id = r.room_id
                    LEFT JOIN games g
                        ON lg.game_id = g.game_id     
            """
        )

    ot.df_to_xl(
        {
            #'room_summary': df_room_agg,
            # 'room_game_summary': df_rooms_games_agg,
            'room_game_data': room_game_data,
            'room_data': room_data
        },
        'C:/users/william.mcguinness/scratch/live_games.xlsx'
    )

def run_job_new():
    with ot.SQLiteHandler('onetime_new.db') as s:
        s.export_db_to_excel(
            'C:/users/william.mcguinness/scratch/onetime_new_db.xlsx'
        )

if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    # run_job()
    run_job_new()