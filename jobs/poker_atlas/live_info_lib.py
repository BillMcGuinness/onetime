import ot

from bs4 import BeautifulSoup, element
from pandas import DataFrame

from pprint import pprint

_BASE_URL = 'https://www.pokeratlas.com'

def get_live_cash_game_html(room_url):
    #raw_url_content = ot.simple_get(room_url)
    # "mock"
    with open('C:/users/william.mcguinness/scratch/atlas.html', 'r') as f:
        raw_url_content = f.read()
        f.close()

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
        df_data, columns=['game', 'table_count', 'waiting_count']
    )
    out_df['update_text'] = last_update_text

    return out_df


def get_live_cash_df(url):
    games_html = get_live_cash_game_html(url)
    out_df = parse_live_cash_game_html_to_df(games_html)
    return out_df

def get_room_info_df():
    url = _BASE_URL + '/poker-rooms/regions/texas'
    raw_url_content = ot.simple_get(url)
    url_content = BeautifulSoup(raw_url_content, 'html.parser')

    rooms = []
    for section in url_content.select('section'):
        if section.get('class') == ['live-venues-list']:
            room_list = section.select('ul')
            for room_list_element in room_list.select('li'):
                room_name = room_list_element.select('a').text.strip()
                url_ext = room_list_element.select('a').get('href')
                room_location = room_list_element.text.strip()
                rooms.append((room_name, url_ext, room_location))

    return DataFrame(
        rooms, columns=['room_name', 'room_url_ext', 'room_location']
    )

def room_data_xform(room_info_df):
    room_info_df = ot.make_id(
        room_info_df, df_cols=['room_name'], id_type='room'
    )

    # todo add job information

    return room_info_df

def live_cash_game_xform(df):
    # todo standardize game names
    # todo get update time based on current time and update text
    return df

if __name__ == '__main__':
    url = 'https://www.pokeratlas.com/poker-room/prime-social-houston'
    cash_html = get_live_cash_game_html(url)
    df = parse_live_cash_game_html_to_df(cash_html)
    pprint(df)