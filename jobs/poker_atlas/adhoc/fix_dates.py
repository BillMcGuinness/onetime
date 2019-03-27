import ot
import pandas as pd
from functools import partial

def run_job():
    with ot.SQLiteHandler('onetime.db') as s:
        bad_date_df = s.sql_to_df(
            query="""
                SELECT *
                FROM live_games lg
                WHERE lg.updated LIKE '1%'
                    --AND lg.update_text != 'Last updated: less than a minute ago'
                --LIMIT 5
            """
        )

    # print(bad_date_df)
    out = ot.parse_df_atlas_update_text(
        bad_date_df, 'updated_new', 'update_text', 'job_timestamp_system'
    )

    del out['updated']
    out['updated'] = out['updated_new'].copy()
    del out['updated_new']

    print(out)
    print(out['updated'].unique())

    # with ot.SQLiteHandler('onetime.db') as s:
    #     s.upsert_df(
    #         df=out, table='live_games', upsert_col='live_game_id'
    #     )

if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    run_job()