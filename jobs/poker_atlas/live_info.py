import ot
from jobs.poker_atlas.live_info_lib import (
    get_room_info_df, room_data_xform, _BASE_URL, get_live_cash_df,
    live_cash_game_xform, create_tables
)
from logging import getLogger

import pandas as pd

log = getLogger()

_DB_NAME = 'onetime.db'

def run_job():
    create_tables(db_name=_DB_NAME)

    room_info_df = get_room_info_df()
    print(room_info_df)
    room_info_df = room_data_xform(room_info_df)
    print(room_info_df)
    with ot.SQLiteHandler(_DB_NAME) as s:
        s.upsert_df(df=room_info_df, table='rooms', upsert_col='room_id')

    all_live_cash_df = pd.DataFrame()
    for idx, row in room_info_df.iterrows():
        log.info('Finding live cash games for {}'.format(row['room_name']))
        room_url_ext = room_info_df['room_url_ext']
        cash_game_url = _BASE_URL + room_url_ext + '/cash-games'
        live_cash_df = get_live_cash_df(cash_game_url)
        print(live_cash_df)
        if not live_cash_df.empty:
            live_cash_df['room_id'] = row['room_id']
        all_live_cash_df = pd.concat(
            [all_live_cash_df, live_cash_df], ignore_index=True
        )

    all_live_cash_df, game_df = live_cash_game_xform(all_live_cash_df)
    print(all_live_cash_df)
    print(game_df)

    with ot.SQLiteHandler(_DB_NAME) as s:
        s.upsert_df(df=game_df, table='games', upsert_col='game_id')
        s.upsert_df(
            df=all_live_cash_df, table='live_games', upsert_col='live_game_id'
        )


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    run_job()