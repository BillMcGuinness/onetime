import ot
from bs4 import BeautifulSoup, element
from pandas import DataFrame
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
    for section in url_content.select('section'):
        if section.get('class') == ['live-venues-list']:
            room_list = section.find('ul')
            for room_list_element in room_list.select('li'):
                a_element = room_list_element.find('a')
                room_name = a_element.text.strip()
                url_ext = a_element.get('href')
                room_location = room_list_element.text
                room_location = room_location.replace(room_name, '')
                room_location = room_location.replace('(', '').replace(')', '')
                room_location = room_location.strip()
                rooms.append((room_name, url_ext, room_location))

    return DataFrame(
        rooms, columns=['room_name', 'room_url_ext', 'room_location']
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
    # url = 'https://www.pokeratlas.com/poker-room/prime-social-houston/cash' \
    #       '-games'
    # cash_html = get_live_cash_game_html(url)
    # df = parse_live_cash_game_html_to_df(cash_html)
    # pprint(df)
    with ot.SQLiteHandler('onetime') as s:
        res = s.table_exists('rooms234')
        print(res)