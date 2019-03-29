import ot
import pandas as pd

def run_job():
    with ot.SQLiteHandler('onetime.db') as s:
        # s.export_db_to_excel(
        #     'C:/users/william.mcguinness/scratch/onetime_db.xlsx'
        # )
        room_data_tables = s.sql_to_df(
            query="""
            SELECT r.room_name || ' (' || r.room_location || ')' as 'room',
                DATETIME(lg.updated) as 'updated',
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
        room_data_members = s.sql_to_df(
            query="""
            SELECT t.room, t.updated, t.player_count, w.unique_waiters, 
                IFNULL(wi.unique_walk_in_waiters, 0) as 
                'unique_walk_in_waiters',
                t.player_count + IFNULL(wi.unique_walk_in_waiters, 0) as 
                'on_prem_members'
            FROM (
                SELECT r.room_name || ' (' || r.room_location || ')' as 'room',
                    DATETIME(lg.updated) as 'updated',
                    SUM(CASE
                        WHEN lg.waiting_count > 0
                            THEN 9*lg.table_count
                        WHEN lg.table_count > 0
                            THEN ((9*(lg.table_count-1)) + 5)
                        ELSE 0
                        END) AS 'player_count'
                FROM live_games lg
                    LEFT JOIN rooms r
                        ON lg.room_id = r.room_id
                    LEFT JOIN games g
                        ON lg.game_id = g.game_id
                GROUP BY r.room_name || ' (' || r.room_location || ')',
                    DATETIME(lg.updated)
            ) t
                LEFT JOIN (
                    SELECT r.room_name || ' (' || r.room_location || ')' as 'room',
                        DATETIME(lg.updated) as 'updated',
                        COUNT(DISTINCT lw.waitlist_name) as 'unique_waiters'
                    FROM live_games lg
                        LEFT JOIN rooms r
                            ON lg.room_id = r.room_id
                        LEFT JOIN live_waitlists lw
                            ON lg.live_game_id = lw.live_game_id
                    GROUP BY r.room_name || ' (' || r.room_location || ')',
                        DATETIME(lg.updated)
                ) w
                    ON t.room = w.room
                        AND t.updated = w.updated
                LEFT JOIN (
                    SELECT r.room_name || ' (' || r.room_location || ')' as 'room',
                        DATETIME(lg.updated) as 'updated',
                        COUNT(DISTINCT lw.waitlist_name) as 
                        'unique_walk_in_waiters'
                    FROM live_games lg
                        LEFT JOIN rooms r
                            ON lg.room_id = r.room_id
                        LEFT JOIN live_waitlists lw
                            ON lg.live_game_id = lw.live_game_id
                    WHERE lw.waitlist_type = 'walk-in'
                    GROUP BY r.room_name || ' (' || r.room_location || ')',
                        DATETIME(lg.updated)
                ) wi
                    ON t.room = wi.room
                        AND t.updated = wi.updated
            WHERE t.updated >= (
                SELECT MIN(job_timestamp_system)
                FROM live_waitlists
            )
            """
        )
        room_game_data = s.sql_to_df(
            query="""
            SELECT r.room_name || ' (' || r.room_location || ')' as 'room',
                g.game_name,  
                DATETIME(lg.updated) as 'updated',
                lg.table_count,
                lg.waiting_count,
                CASE
                    WHEN lg.waiting_count > 0
                        THEN ((9*lg.table_count) + lg.waiting_count)
                    WHEN lg.table_count > 0
                        THEN ((9*(lg.table_count-1)) + 5)
                    ELSE 0
                    END AS 'interested_member_count'
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
            'room_data_tables': room_data_tables,
            'room_data_members': room_data_members
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