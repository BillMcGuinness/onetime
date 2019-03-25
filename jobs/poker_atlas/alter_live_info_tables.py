import ot
import pandas as pd

_DB_NAME = 'onetime.db'

def run_job():
    with ot.SQLiteHandler(_DB_NAME) as s:
        s.add_columns(
            table='rooms', cols_dtype_map={
                'street_address': 'text',
                'address_locality': 'text',
                'postal_code': 'text',
                'telephone': 'text',
                'min_age': 'integer',
                'total_tables': 'integer',
                'hours': 'text',
                'has_jackpots': 'integer',
                'has_phone_in': 'integer',
                'has_cell_reception': 'integer',
                'has_food_tableside': 'integer',
                'has_check_cashing': 'integer',
                'has_order_food_at_table': 'integer',
                'has_rv_parking': 'integer',
                'has_televisions': 'integer',
                'has_wifi': 'integer',
                'has_massage': 'integer',
                'has_comps': 'integer',
                'has_nearby_restrooms': 'integer',
                'non_smoking': 'integer',
                'has_auto_shufflers': 'integer',
                'has_cocktail_service': 'integer',
                'has_currency_exchange': 'integer',
                'has_cash_games': 'integer',
                'has_valet_parking': 'integer',
                'has_discounted_hotel_rates': 'integer',
                'venue_type': 'text',
                'rewards_program': 'text',
                'comps_promos': 'text'
            }
        )
        # df = s.sql_to_df(
        #     query="""
        #         SELECT *
        #         FROM rooms
        #     """
        # )
        # print(df.shape)
        # print(df.columns)


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    run_job()