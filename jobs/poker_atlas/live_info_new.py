import ot
import time
import random
import optparse
from jobs.poker_atlas.live_info_lib_new import (
    get_room_info_df, room_data_xform, _BASE_URL, get_live_cash_df,
    live_cash_game_xform, create_tables
)
import pandas as pd
import datetime
from pathlib import Path
import os

_LOG_LOCATION = 'C:/users/william.mcguinness/scratch/atlas_log/live_info_log.txt'
_ERROR_LOCATION = 'C:/users/william.mcguinness/scratch/atlas_log/ERROR_live_info_log.txt'

#err_log = ot.get_error_file_logger(_ERROR_LOCATION)
log = ot.get_logger()

_DB_NAME = 'onetime_new.db'

def run_job():
    # raise Exception('test_exception')
    create_tables(db_name=_DB_NAME)


    # start_wait = random.random()*60 + 30
    # log.info('Pausing {} seconds before beginning...'.format(int(start_wait)))
    # time.sleep(start_wait)

    room_info_df = get_room_info_df()
    room_info_df = room_data_xform(room_info_df)
    with ot.SQLiteHandler(_DB_NAME) as s:
        s.upsert_df(df=room_info_df, table='rooms', upsert_col='room_id')

    all_live_cash_df = pd.DataFrame()
    for idx, row in room_info_df.iterrows():
        log.info('Finding live cash games for {}'.format(row['room_name']))
        time_to_wait = random.random()*5 + 3
        log.info(
            'Waiting {} seconds before room call'.format(int(time_to_wait))
        )
        time.sleep(time_to_wait)
        room_url_ext = row['room_url_ext']
        cash_game_url = _BASE_URL + room_url_ext + '/cash-games'
        live_cash_df = get_live_cash_df(cash_game_url)
        if not live_cash_df.empty:
            live_cash_df['room_id'] = row['room_id']
            all_live_cash_df = pd.concat(
                [all_live_cash_df, live_cash_df], ignore_index=True
            )

    all_live_cash_df, game_df = live_cash_game_xform(all_live_cash_df)

    with ot.SQLiteHandler(_DB_NAME) as s:
        s.upsert_df(df=game_df, table='games', upsert_col='game_id')
        s.upsert_df(
            df=all_live_cash_df, table='live_games', upsert_col='live_game_id'
        )

def process_command_line():
    optp = optparse.OptionParser()
    optp.add_option(
        '--cleanup', dest='cleanup', help='Clean upLog File', default='False'
    )
    opts, args = optp.parse_args()

    if opts.cleanup == 'True':
        opts.cleanup = True
    else:
        opts.cleanup = False

    return opts

def cleanup_log(cleanup=False):
    if cleanup:
        file_ext = str(datetime.datetime.now()).replace('-', '_').replace(
            ':', ''
        ).replace('.', '').replace(' ', '')

        # out_log_loc = _LOG_LOCATION.replace('.txt', '{}.txt'.format(file_ext))
        # with open(_LOG_LOCATION) as f:
        #     with open(out_log_loc, "w") as f1:
        #         for line in f:
        #             f1.write(line)

        out_error_loc = _ERROR_LOCATION.replace('.txt', '{}.txt'.format(file_ext))
        if os.path.isfile(_ERROR_LOCATION):
            if os.stat(_ERROR_LOCATION).st_size > 0:
                with open(_ERROR_LOCATION) as f:
                        with open(out_error_loc, "w") as f1:
                            for line in f:
                                f1.write(line)



if __name__ == '__main__':
    pd.set_option('display.max_columns', None)

    opts = process_command_line()

    # opts.cleanup = True

    try:
        run_job()
    except Exception as e:
        #err_log.error(e)
        log.error()
    cleanup_log(opts.cleanup)