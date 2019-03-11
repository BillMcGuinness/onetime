import ot
from jobs.poker_atlas.live_info_lib import (
    get_room_info_df, room_data_xform, _BASE_URL, get_live_cash_df,
    live_cash_game_xform
)

import pandas as pd

def run_job():
    room_info_df = get_room_info_df()
    room_info_df = room_data_xform(room_info_df)
    # todo load room_info_df to table

    all_live_cash_df = pd.DataFrame()
    for idx, row in room_info_df.iterrows():
        room_url_ext = room_info_df['room_url_ext']
        cash_game_url = _BASE_URL + room_url_ext + '/cash-games'
        live_cash_df = get_live_cash_df(cash_game_url)
        live_cash_df['room_id'] = row['room_id']
        all_live_cash_df = pd.concat(
            [all_live_cash_df, live_cash_df], ignore_index=True
        )

    all_live_cash_df = live_cash_game_xform(all_live_cash_df)


if __name__ == '__main__':
    run_job()