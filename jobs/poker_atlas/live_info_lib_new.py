import ot
from bs4 import BeautifulSoup, element
from pandas import DataFrame, set_option
from numpy import int64

from pprint import pprint

log = ot.get_logger()

_BASE_URL = 'https://www.pokeratlas.com'

def get_live_cash_game_html(room_url):
    raw_url_content = ot.simple_get(room_url)
    # "mock"
    # with open('C:/users/william.mcguinness/scratch/atlas.html', 'r') as f:
    #     raw_url_content = f.read()
    #     f.close()

    url_content = BeautifulSoup(raw_url_content, 'html.parser')

    for tbl_html in url_content.select('table'):
        if tbl_html.get('class') == ['live-info']:
            row_classes = [i.get('class') for i in tbl_html.select('tr')]
            if 'live-cash-game' in ot.flatten_list(row_classes):
                return tbl_html

    return None

def parse_live_cash_game_html_to_df(cash_html):
    assert isinstance(cash_html, element.Tag)
    df_data = []
    last_update_text = None
    for row in cash_html.select('tr'):
        row_class = row.get('class')
        if row_class:
            if 'live-cash-game' in row_class:
                row_data = []
                for cell in row.select('td'):
                    if cell.text.strip():
                        row_data.append(cell.text.strip())
                df_data.append(tuple(row_data))
        else:
            for non_live_game_cell in row.select('td'):
                non_live_game_cell_class = non_live_game_cell.get('class')
                if non_live_game_cell_class:
                    if 'last-update' in non_live_game_cell_class:
                        last_update_text = non_live_game_cell.text

    if not last_update_text:
        last_update_text = 'Last updated: 1 minute ago'

    out_df = DataFrame.from_records(
        df_data, columns=['game_name_raw', 'table_count', 'waiting_count']
    )
    out_df['update_text'] = last_update_text

    out_df['table_count'] = out_df['table_count'].astype(int64)
    out_df['waiting_count'] = out_df['waiting_count'].astype(int64)

    return out_df


def get_live_cash_df(url):
    games_html = get_live_cash_game_html(url)
    if games_html:
        out_df = parse_live_cash_game_html_to_df(games_html)
    else:
        log.warning('Could not parse live cash game table for {}'.format(url))
        out_df = DataFrame()
    return out_df

def get_room_info_df():
    url = _BASE_URL + '/poker-rooms/regions/texas'
    raw_url_content = ot.simple_get(url)
    url_content = BeautifulSoup(raw_url_content, 'html.parser')

    rooms = []
    live_venue_section = url_content.find('section', {'class': 'live-venues-list'})
    room_list = live_venue_section.find('ul')
    for room_list_element in room_list.select('li'):
        a_element = room_list_element.find('a')
        room_name = a_element.text.strip()
        room_location = room_list_element.text
        room_location = room_location.replace(room_name, '')
        room_location = room_location.replace('(', '').replace(')', '')
        room_location = room_location.strip()

        url_ext = a_element.get('href')
        room_url = _BASE_URL + url_ext
        raw_room_url_content = ot.simple_get(room_url)
        room_url_content = BeautifulSoup(
            raw_room_url_content, 'html.parser'
        )
        street_addr = room_url_content.find(
            'span', {'itemprop':'streetAddress'}
        ).text.strip()
        addr_locality = room_url_content.find(
            'span', {'itemprop': 'addressLocality'}
        ).text.strip()
        addr_postal = room_url_content.find(
            'span', {'itemprop': 'postalCode'}
        ).text.strip()

        try:
            telephone = room_url_content.find(
                'a', {'itemprop': 'telephone'}
            ).text.strip()
        except AttributeError:
            telephone = None

        table_hours_age = room_url_content.find(
            'dl', {'class': 'tables-hours-age'}
        ).select('dd')
        min_age = int(table_hours_age[0].text.strip())
        total_tables = table_hours_age[1].text.strip()
        total_tables = int(total_tables.lower().replace('tables', '').strip())
        hours = table_hours_age[2].text.strip()

        try:
            has_jackpots = 1 if room_url_content.find(
                'input', {'id':'has_jackpots_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_jackpots = 0
        try:
            has_rv_parking = 1 if room_url_content.find(
                'input', {'id': 'has_rv_parking_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_rv_parking = 0
        try:
            non_smoking = 1 if room_url_content.find(
                'input', {'id': 'has_non_smoking_cd'}
            ).get('checked') else 0
        except AttributeError:
            non_smoking = 0
        try:
            has_televisions = 1 if room_url_content.find(
                'input', {'id': 'has_televisions_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_televisions = 0
        try:
            has_auto_shufflers = 1 if room_url_content.find(
                'input', {'id': 'has_auto_shufflers_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_auto_shufflers = 0
        try:
            has_cell_reception = 1 if room_url_content.find(
                'input', {'id': 'has_cell_reception_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_cell_reception = 0
        try:
            has_wifi = 1 if room_url_content.find(
                'input', {'id': 'has_wifi_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_wifi = 0
        try:
            has_cocktail_service = 1 if room_url_content.find(
                'input', {'id': 'has_cocktail_service_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_cocktail_service = 0
        try:
            has_food_tableside = 1 if room_url_content.find(
                'input', {'id': 'has_food_tableside_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_food_tableside = 0
        try:
            has_massage = 1 if room_url_content.find(
                'input', {'id': 'has_massage_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_massage = 0
        try:
            has_currency_exchange = 1 if room_url_content.find(
                'input', {'id': 'has_currency_exchange_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_currency_exchange = 0
        try:
            has_check_cashing = 1 if room_url_content.find(
                'input', {'id': 'has_check_cashing_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_check_cashing = 0
        try:
            has_discounted_hotel_rates = 1 if room_url_content.find(
                'input', {'id': 'has_discounted_hotel_rates_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_discounted_hotel_rates = 0
        try:
            has_cash_games = 1 if room_url_content.find(
                'input', {'id': 'has_cash_games_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_cash_games = 0
        try:
            has_nearby_restrooms = 1 if room_url_content.find(
                'input', {'id': 'has_restrooms_near_room_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_nearby_restrooms = 0
        try:
            has_valet_parking = 1 if room_url_content.find(
                'input', {'id': 'has_valet_parking_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_valet_parking = 0
        try:
            has_phone_in = 1 if room_url_content.find(
                'input', {'id': 'has_phone_in_list_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_phone_in = 0
        try:
            has_comps = 1 if room_url_content.find(
                'input', {'id': 'has_comps_offered_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_comps = 0
        try:
            has_order_food_at_table = 1 if room_url_content.find(
                'input', {'id': 'has_order_food_at_table_cd'}
            ).get('checked') else 0
        except AttributeError:
            has_order_food_at_table = 0

        room_details = room_url_content.find(
            'section', {'class': 'room-details'}
        ).select('dd')
        try:
            venue_type = room_details[0].text.strip()
        except IndexError:
            venue_type = None
        try:
            rewards_program = room_details[1].text.strip()
        except IndexError:
            rewards_program = None
        try:
            comps_promos = room_details[2].text.strip()
        except IndexError:
            comps_promos = None


        rooms.append((
            room_name, url_ext, room_location, street_addr, addr_locality,
            addr_postal, telephone, min_age, total_tables, hours, has_jackpots,
            has_rv_parking, non_smoking, has_televisions, has_auto_shufflers,
            has_cell_reception, has_wifi, has_cocktail_service,
            has_food_tableside, has_massage, has_currency_exchange,
            has_check_cashing, has_discounted_hotel_rates, has_cash_games,
            has_nearby_restrooms, has_valet_parking, has_phone_in, has_comps,
            has_order_food_at_table, venue_type, rewards_program, comps_promos
        ))

    return DataFrame(
        rooms, columns=[
            'room_name', 'room_url_ext', 'room_location', 'street_address',
            'address_locality', 'postal_code', 'telephone', 'min_age',
            'total_tables', 'hours', 'has_jackpots', 'has_rv_parking',
            'non_smoking', 'has_televisions', 'has_auto_shufflers',
            'has_cell_reception', 'has_wifi', 'has_cocktail_service',
            'has_food_tableside', 'has_massage', 'has_currency_exchange',
            'has_check_cashing', 'has_discounted_hotel_rates', 'has_cash_games',
            'has_nearby_restrooms', 'has_valet_parking', 'has_phone_in',
            'has_comps', 'has_order_food_at_table', 'venue_type',
            'rewards_program', 'comps_promos'
        ]
    )

def room_data_xform(room_info_df):
    room_info_df = ot.make_id(
        room_info_df, df_cols=['room_name'], id_type='room'
    )

    room_info_df = ot.add_job_info(room_info_df, __file__)

    return room_info_df

def live_cash_game_xform(df):
    df = ot.add_job_info(df, __file__)

    df['game_name'] = df['game_name_raw'].apply(ot.standardize_game_name)

    df = ot.make_id(df, df_cols=['game_name'], id_type='game')
    game_df = df[[
        'game_id', 'game_name_raw', 'game_name', 'job_source',
        'job_timestamp_system', 'job_timestamp_utc'
    ]].copy()
    game_df.drop_duplicates(subset='game_id', inplace=True)

    df.drop(['game_name_raw', 'game_name'], axis=1, inplace=True)

    df['updated'] = df['update_text'].apply(ot.parse_atlas_update_text)

    df = ot.make_id(
        df, df_cols=['room_id', 'game_id', 'updated'], id_type='live_game'
    )

    return df, game_df

def create_tables(db_name):
    log.info('Setting up tables')
    create_room_table(db_name)
    create_game_table(db_name)
    create_live_game_table(db_name)

def create_room_table(db_name):
    with ot.SQLiteHandler(db_name) as sqlh:
        if not sqlh.table_exists('rooms'):
            log.info("rooms table doesn't exist, creating rooms table")
            sqlh.create_table(
                table='rooms',
                col_type_maps={
                    'room_id': 'text',
                    'room_name': 'text',
                    'room_url_ext': 'text',
                    'room_location': 'text',
                    'job_source': 'text',
                    'job_timestamp_system': 'text',
                    'job_timestamp_utc': 'text',
                    'street_address': 'text',
                    'address_locality': 'text',
                    'telephone': 'text',
                    'min_age': 'text',
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
                    'venue_type': 'text',
                    'rewards_program': 'text',
                    'comps_promos': 'text'
                }
            )
        else:
            log.info('rooms table already exists, skipping table creation')

def create_game_table(db_name):
    with ot.SQLiteHandler(db_name) as sqlh:
        if not sqlh.table_exists('games'):
            log.info("games table doesn't exist, creating rooms table")
            sqlh.create_table(
                table='games',
                col_type_maps={
                    'game_id': 'text',
                    'game_name': 'text',
                    'game_name_raw': 'text',
                    'job_source': 'text',
                    'job_timestamp_system': 'text',
                    'job_timestamp_utc': 'text',
                }
            )
        else:
            log.info('games table already exists, skipping table creation')

def create_live_game_table(db_name):
    with ot.SQLiteHandler(db_name) as sqlh:
        if not sqlh.table_exists('live_games'):
            log.info("live_games table doesn't exist, creating rooms table")
            sqlh.create_table(
                table='live_games',
                col_type_maps={
                    'room_id': 'text',
                    'game_id': 'text',
                    'live_game_id': 'text',
                    'table_count': 'integer',
                    'waiting_count': 'integer',
                    'update_text': 'text',
                    'updated': 'text',
                    'job_source': 'text',
                    'job_timestamp_system': 'text',
                    'job_timestamp_utc': 'text',
                }
            )
        else:
            log.info('live_games table already exists, skipping table creation')

if __name__ == '__main__':
    set_option('display.max_columns', None)
    room_df = get_room_info_df()
    print(room_df)