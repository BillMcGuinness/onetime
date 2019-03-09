import ot

from bs4 import BeautifulSoup, element
from pandas import DataFrame

from pprint import pprint



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
    for row in cash_html.select('tr'):
        row_class = row.get('class')
        if row_class:
            if 'live-cash-game' in row_class:
                row_data = []
                for cell in row.select('td'):
                    if cell.text.strip():
                        row_data.append(cell.text.strip())
                df_data.append(tuple(row_data))

    return DataFrame.from_records(
        df_data, columns=['game', 'table_count', 'waiting_count']
    )


if __name__ == '__main__':
    url = 'https://www.pokeratlas.com/poker-room/prime-social-houston'
    cash_html = get_live_cash_game_html(url)
    df = parse_live_cash_game_html_to_df(cash_html)
    pprint(df)