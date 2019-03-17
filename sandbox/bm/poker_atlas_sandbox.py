import ot
import pandas as pd

def infer_member_count_per_game(row):
    if row['waiting_count'] > 0:
        row['member_count'] = (row['table_count'] * 9) + row['waiting_count']
    else:
        # todo use past data to infer players at each table
        # for now, assume 5 players each
        row['member_count'] = row['table_count'] * 5
    return row

def group_by_room(df):

    # out_df = pd.DataFrame(pd.to_datetime(df['updated']))
    # out_df = out_df.groupby()
    df['updated'] = pd.to_datetime(df['updated'])
    room_group = df.groupby(['room_name', ])

    #def _

def run_job():
    with ot.SQLiteHandler('onetime.db') as s:
        # s.export_db_to_excel(
        #     'C:/users/william.mcguinness/scratch/onetime_db.xlsx'
        # )
        df_room_total = s.sql_to_df(
            query="""
            /*SELECT t.room, t.updated, SUM(t.member_count) as 'member_count'
            FROM (*/
                SELECT r.room_name || ' (' || r.room_location || ')' as 'room',  
                    time(lg.updated) as 'updated',
                    SUM(CASE
                        WHEN lg.waiting_count > 0
                            THEN (9*lg.table_count) + lg.waiting_count
                        ELSE
                            5*lg.table_count
                        END) AS 'member_count'
                FROM live_games lg
                    LEFT JOIN rooms r
                        ON lg.room_id = r.room_id
                    LEFT JOIN games g
                        ON lg.game_id = g.game_id
                GROUP BY r.room_name || ' (' || r.room_location || ')',
                    --g.game_name, 
                    time(lg.updated)
                /*) t
                GROUP BY t.room, t.updated */          
            """
        )
        df_rooms_games_total = s.sql_to_df(
            query="""
            SELECT r.room_name || ' (' || r.room_location || ')' as 'room',
                g.game_name,  
                    time(lg.updated) as 'updated',
                    SUM(CASE
                        WHEN lg.waiting_count > 0
                            THEN (9*lg.table_count) + lg.waiting_count
                        ELSE
                            5*lg.table_count
                        END) AS 'member_count'
                FROM live_games lg
                    LEFT JOIN rooms r
                        ON lg.room_id = r.room_id
                    LEFT JOIN games g
                        ON lg.game_id = g.game_id
                GROUP BY r.room_name || ' (' || r.room_location || ')',
                    g.game_name, 
                    time(lg.updated)         
            """
        )

    #df = df.apply(infer_member_count_per_game, axis=1)

    #df = group_by_room(df)

    ot.df_to_xl(
        {'room_summary': df_room_total, 'room_game_summary': df_rooms_games_total},
        'C:/users/william.mcguinness/scratch/live_games.xlsx'
    )



if __name__ == '__main__':
    # pd.set_option('display.max_columns', None)
    run_job()