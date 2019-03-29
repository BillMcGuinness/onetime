import ot
from bs4 import BeautifulSoup, element
from pandas import DataFrame, set_option, concat, merge
from numpy import int64
import dateutil.parser as dup

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

    out = {}

    for tbl_html in url_content.select('table'):
        if tbl_html.get('class') == ['live-info']:
            row_classes = [i.get('class') for i in tbl_html.select('tr')]
            if 'live-cash-game' in ot.flatten_list(row_classes):
                out['live_games'] = tbl_html

    out['live_waitlists'] = url_content.findAll(
        'div', {'class': 'modal-overlay square-corners'}
    )

    if 'live_games' not in out:
        out['live_games'] = None

    return out

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
                    else:
                        cell_div = cell.find('div')
                        if cell_div:
                            row_data.append(cell_div.get('data-modal'))
                        else:
                            row_data.append(None)
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
        df_data, columns=[
            'game_name_raw', 'table_count', 'waiting_count', 'waitlist_modal_id'
        ]
    )
    out_df['update_text'] = last_update_text

    out_df['table_count'] = out_df['table_count'].astype(int64)
    out_df['waiting_count'] = out_df['waiting_count'].astype(int64)

    return out_df

def parse_live_waitlist_html_to_df(waitlist_htmls):
    all_waitlist_df_list = []
    for waitlist_html in waitlist_htmls:
        this_waitlist_df_records = []
        if waitlist_html.find('div', {'class': 'live-waitlist'}):
            waitlist_id = waitlist_html.get('id')
            for list_item in waitlist_html.find(
                    'div', {'class': 'waitlist'}
            ).select('li'):
                waitlist_index = list_item.find('div', {'class': 'index'}).text
                waitlist_type = list_item.find(
                    'div', {'class': 'name'}
                ).get('class')[1]
                waitlist_name = list_item.find(
                    'div', {'class': 'name'}
                ).text.strip()
                this_waitlist_df_records.append(
                    (waitlist_index, waitlist_type, waitlist_name)
                )
            if this_waitlist_df_records:
                this_waitlist_df = DataFrame.from_records(
                    this_waitlist_df_records, columns=[
                        'waitlist_index', 'waitlist_type', 'waitlist_name'
                    ]
                )
                this_waitlist_df['waitlist_modal_id'] = waitlist_id
            else:
                this_waitlist_df = DataFrame()

            if not this_waitlist_df.empty:
                all_waitlist_df_list.append(this_waitlist_df)

    if all_waitlist_df_list:
        out_df = concat(all_waitlist_df_list, ignore_index=True)
    else:
        out_df = DataFrame(
            columns=[
                'waitlist_index', 'waitlist_type', 'waitlist_name',
                'waitlist_modal_id'
            ]
        )
    return out_df

def get_live_cash_df(url):
    games_and_waitlist_html = get_live_cash_game_html(url)
    games_html = games_and_waitlist_html['live_games']
    waitlist_htmls = games_and_waitlist_html['live_waitlists']
    if games_html:
        out_games_df = parse_live_cash_game_html_to_df(games_html)
        out_waitlist_df = parse_live_waitlist_html_to_df(waitlist_htmls)
    else:
        log.warning('Could not parse live cash game table for {}'.format(url))
        out_games_df = DataFrame()
        out_waitlist_df = DataFrame()
    return out_games_df, out_waitlist_df

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

def get_this_tournament_dfs(main_tournament_html, room_id):
    this_tournament_df = parse_tournament_df(main_tournament_html)
    this_structure_df = parse_structure_df(main_tournament_html)
    this_payout_df = parse_payout_df(main_tournament_html)

    structure_id = ot.hash_object(this_structure_df)
    this_structure_df['tournament_structure_id'] = structure_id
    this_tournament_df['tournament_structure_id'] = structure_id

    this_tournament_df['room_id'] = room_id
    this_tournament_df = ot.make_id(
        this_tournament_df, df_cols=['room_id', 'name', 'start_datetime'],
        id_type='tournament'
    )
    this_tournament_id = this_tournament_df['tournament_id'][0]

    this_payout_df['tournament_id'] = this_tournament_id

    return this_tournament_df, this_structure_df, this_payout_df

def get_tournament_dfs(url, room_id):
    all_tournament_df = DataFrame()
    all_structure_df = DataFrame()
    all_payout_df = DataFrame()

    raw_tournament_landing_html = ot.simple_get(url)
    tournament_landing_html = BeautifulSoup(
        raw_tournament_landing_html, 'html.parser'
    )

    live_tournaments_htmls = tournament_landing_html.findAll(
        'tr', {'class': 'live-tournament'}
    )

    if live_tournaments_htmls:
        for live_tournament_html in live_tournaments_htmls:
            main_tournament_html = get_main_tournament_html(
                live_tournament_html
            )
            this_tournament_df, this_structure_df, this_payout_df = \
                get_this_tournament_dfs(main_tournament_html, room_id)

            all_tournament_df = concat(
                [all_tournament_df, this_tournament_df], ignore_index=True
            )
            all_structure_df = concat(
                [all_structure_df, this_structure_df], ignore_index=True
            )
            all_payout_df = concat(
                [all_payout_df, this_payout_df], ignore_index=True
            )

    upcoming_tournaments_html = tournament_landing_html.find(
        'section', {'class': 'upcoming-tournaments'}
    )

    if upcoming_tournaments_html:
        for upcoming_tournament_html in upcoming_tournaments_html.select('li'):
            main_tournament_html = get_main_tournament_html(
                upcoming_tournament_html
            )
            this_tournament_df, this_structure_df, this_payout_df = \
                get_this_tournament_dfs(main_tournament_html, room_id)

            all_tournament_df = concat(
                [all_tournament_df, this_tournament_df], ignore_index=True
            )
            all_structure_df = concat(
                [all_structure_df, this_structure_df], ignore_index=True
            )
            all_payout_df = concat(
                [all_payout_df, this_payout_df], ignore_index=True
            )

    return all_tournament_df, all_structure_df, all_payout_df

def parse_tournament_df(html):
    name = html.find('div', {'class':'meta'}).find('h1').text.strip()

    start_date_raw = html.find(
        'div', {'class':'meta'}
    ).find('div', {'class':'date'}).text.strip()
    start_time_raw = html.find(
        'div', {'class':'meta'}
    ).find('div', {'class':'time'}).find('span').text.strip()
    start_datetime = str(dup.parse(start_date_raw + ' ' + start_time_raw))

    tournament_game_name_raw = html.find(
        'div', {'class':'game'}
    ).find('span').text.strip()

    reg_details_labels = html.find(
        'div', {'class': 'registration-details'}
    ).findAll('dt')
    reg_details = html.find(
        'div', {'class': 'registration-details'}
    ).findAll('dd')
    reg_open_datetime = None
    reg_close_datetime = None
    reg_comments = None
    for reg_label, reg_detail in zip(
        reg_details_labels, reg_details
    ):
        label_text = reg_label.text.lower()
        detail_text = reg_detail.text.strip()
        if 'registration open' in label_text:
            reg_open_time_raw = detail_text
            reg_open_datetime = str(dup.parse(
                start_date_raw + ' ' + reg_open_time_raw
            ))
        elif 'registration close' in label_text:
            reg_close_time_raw = detail_text
            reg_close_datetime = str(dup.parse(
                start_date_raw + ' ' + reg_close_time_raw
            ))
        elif not label_text:
            reg_comments = detail_text

    buy_in_details_labels = html.find(
        'div', {'class':'buy-in-details'}
    ).findAll('dt')
    buy_in_details = html.find('div', {'class':'buy-in-details'}).findAll('dd')
    total_buy_in = None
    entry_fee = None
    deductions = None
    for buy_in_label, buy_in_detail in zip(
        buy_in_details_labels, buy_in_details
    ):
        buy_in_detail_clean = buy_in_detail.text.replace('$','').replace(
            ',', ''
        ).strip()
        if 'buy-in' in buy_in_label.text.lower():
            total_buy_in = buy_in_detail_clean
        elif 'entry' in buy_in_label.text.lower():
            entry_fee = buy_in_detail_clean
        elif 'deduction' in buy_in_label.text.lower():
            deductions = buy_in_detail_clean

    format_details_labels = html.find(
        'div', {'class':'format-details'}
    ).findAll('dt')
    format_details = html.find('div', {'class':'format-details'}).findAll('dd')
    starting_chips = None
    starting_blinds = None
    starting_small_blind = None
    starting_big_blind = None
    ante_type = None
    staff_bonus = None
    staff_bonus_chips = None
    reentry = None
    rebuys = None
    rebuy_cost = None
    rebuy_chips = None
    addons = None
    addon_cost = None
    addon_chips = None
    bounties = None
    for format_label, format_detail in zip(
        format_details_labels, format_details
    ):
        label_text = format_label.text.lower()
        detail_text = format_detail.text.strip()
        if 'starting chips' in label_text:
            starting_chips = detail_text.replace(',', '').strip()
        elif 'starting blind' in label_text:
            starting_blinds = detail_text
            starting_small_blind = detail_text.split('/')[0]
            starting_big_blind = detail_text.split('/')[1]
        elif 'ante type' in label_text:
            ante_type = detail_text
        elif 'staff bonus' in label_text and 'chips' not in label_text:
            staff_bonus = detail_text.replace('$', '').replace(',', '').strip()
        elif 'staff bonus chip' in label_text:
            staff_bonus_chips = detail_text.replace(',', '').strip()
        elif 're-entry' in label_text:
            reentry = detail_text
        elif 'rebuys' in label_text:
            rebuys = detail_text
        elif 'rebuy cost'in label_text:
            rebuy_cost = detail_text.replace('$', '').replace(',', '').strip()
        elif 'rebuy chip' in label_text:
            rebuy_chips = detail_text.replace(',', '').strip()
        elif 'addons' in label_text:
            addons= detail_text
        elif 'addon cost' in label_text:
            addon_cost = detail_text.replace('$', '').replace(',', '').strip()
        elif 'addon chip' in label_text:
            addon_chips = detail_text.replace(',', '').strip()
        elif 'bounties' in label_text:
            bounties = detail_text

    size_details_labels = html.find(
        'div', {'class': 'size-details'}
    ).findAll('dt')
    size_details = html.find('div', {'class': 'size-details'}).findAll('dd')
    guarantee = None
    added_money = None
    for size_label, size_detail in zip(
        size_details_labels, size_details
    ):
        label_text = size_label.text.lower()
        detail_text = size_detail.text.strip()
        if 'guarantee' in label_text:
            if 'none' in detail_text.lower():
                guarantee = 0
            else:
                guarantee = detail_text.replace('$', '').replace(',', '').strip()
        elif 'added money' in label_text:
            added_money = detail_text.replace('$', '').replace(',', '').strip()

    structure_details_labels = html.find(
        'div', {'class': 'structure-details'}
    ).findAll('dt')
    structure_details = html.find('div', {'class': 'structure-details'}).findAll('dd')
    level_time_mins = None
    break_length_mins = None
    break_frequency = None
    for structure_label, structure_detail in zip(
        structure_details_labels, structure_details
    ):
        label_text = structure_label.text.lower()
        detail_text = structure_detail.text.strip()
        if 'level time' in label_text:
            level_time_mins = detail_text.replace('minutes', '').replace(
                'min', ''
            ).strip()
        elif 'break length' in label_text:
            break_length_mins = detail_text.replace('minutes', '').replace(
                'min', ''
            ).strip()
        elif 'break freq' in label_text:
            break_frequency = detail_text


    df_data = (
        name, start_datetime, reg_open_datetime, reg_close_datetime,
        reg_comments, tournament_game_name_raw, total_buy_in, entry_fee,
        deductions, starting_chips, starting_blinds, starting_small_blind,
        starting_big_blind, ante_type, staff_bonus, staff_bonus_chips,
        reentry, rebuys, rebuy_cost, rebuy_chips, addons, addon_cost,
        addon_chips, bounties, guarantee, added_money, level_time_mins,
        break_length_mins, break_frequency
    )
    return DataFrame.from_records(
        [df_data], columns=[
            'name', 'start_datetime', 'reg_open_datetime', 'reg_close_datetime',
            'reg_comments', 'tournament_game_name_raw', 'total_buy_in',
            'entry_fee', 'deductions', 'starting_chips', 'starting_blinds',
            'starting_small_blind', 'starting_big_blind', 'ante_type',
            'staff_bonus', 'staff_bonus_chips', 'reentry', 'rebuys',
            'rebuy_cost', 'rebuy_chips', 'addons', 'addon_cost', 'addon_chips',
            'bounties', 'guarantee', 'added_money', 'level_time_mins',
            'break_length_mins', 'break_frequency'
        ]
    )

def parse_structure_df(html):
    tables = html.findAll('table')
    structure_table_idx = None
    for idx, table in enumerate(tables):
        if 'small blind' in [h.text.lower() for h in table.select('th')]:
            structure_table_idx = idx + 1
            break

    if not structure_table_idx:
        log.info('Could not parse tournament structure df')
        return DataFrame()

    structure_table_html = tables[structure_table_idx]
    df_data = []
    for row_idx, row in enumerate(structure_table_html.findAll('tr'), start=1):
        row_data = []
        row_data.append(row_idx)
        for cell in row.findAll('td'):
            row_data.append(cell.text.strip())
        df_data.append(tuple(row_data))

    return DataFrame.from_records(
        df_data, columns=[
            'level_index', 'level_name', 'level_length_mins',
            'level_small_blind', 'level_big_blind', 'level_ante'
        ]
    )

def parse_payout_df(html):
    tables = html.findAll('table')
    payout_table_idx = None
    for idx, table in enumerate(tables):
        if 'payout' in [h.text.lower() for h in table.select('th')]:
            payout_table_idx = idx + 1
            break

    if not payout_table_idx:
        log.info('Could not parse tournament payout df')
        return DataFrame()

    payout_table_html = tables[payout_table_idx]
    df_data = []
    for row_idx, row in enumerate(payout_table_html.findAll('tr'), start=1):
        row_data = []
        row_data.append(row_idx)
        for cell in row.findAll('td'):
            row_data.append(cell.text.strip())
        df_data.append(tuple(row_data))

    return DataFrame.from_records(
        df_data, columns=['payout_index', 'place', 'payout']
    )

def get_main_tournament_html(upcoming_tournament_html):
    main_tournament_url_ext = upcoming_tournament_html.find('a').get('href')
    raw_main_tournament_html = ot.simple_get(
        _BASE_URL + main_tournament_url_ext
    )
    return BeautifulSoup(raw_main_tournament_html, 'html.parser')


def tournament_xform(df):
    df.drop_duplicates(subset='tournament_id', inplace=True)

    df = ot.add_job_info(df, __file__)

    df['tournament_game_name'] = df['tournament_game_name_raw'].apply(
        ot.standardize_game_name
    )

    df = ot.make_id(
        df, df_cols=['tournament_game_name'], id_type='tournament_game'
    )
    game_df = df[[
        'tournament_game_id', 'tournament_game_name_raw',
        'tournament_game_name', 'job_source', 'job_timestamp_system',
        'job_timestamp_utc'
    ]].copy()
    game_df.drop_duplicates(subset='tournament_game_id', inplace=True)

    df.drop(
        ['tournament_game_name_raw', 'tournament_game_name'], axis=1,
        inplace=True
    )

    return df, game_df

def tournament_structure_xform(df):
    df = ot.add_job_info(df, __file__)

    df = ot.make_id(
        df,
        df_cols=['tournament_structure_id', 'level_index'],
        id_type='tournament_structure_level'
    )

    df.drop_duplicates(subset='tournament_structure_level_id', inplace=True)

    return df

def tournament_payout_xform(df):
    df = ot.add_job_info(df, __file__)

    df = ot.make_id(
        df, df_cols=['tournament_id', 'payout_index'],
        id_type='tournament_payout_place'
    )

    df.drop_duplicates(subset='tournament_payout_place_id', inplace=True)

    df['payout'] = df['payout'].str.replace('$', '').str.replace(
        ',', ''
    ).str.strip()

    return df

def room_data_xform(room_info_df):
    room_info_df = ot.make_id(
        room_info_df, df_cols=['room_name'], id_type='room'
    )

    room_info_df = ot.add_job_info(room_info_df, __file__)

    return room_info_df

def live_cash_game_xform(df, waitlist_df):
    df = ot.add_job_info(df, __file__)
    waitlist_df = ot.add_job_info(waitlist_df, __file__)

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

    waitlist_df = merge(
        waitlist_df, df[['live_game_id', 'waitlist_modal_id', 'room_id']],
        how='left',
        on=['waitlist_modal_id', 'room_id']
    )

    waitlist_df = ot.make_id(
        waitlist_df, df_cols=[
            'live_game_id',  'waitlist_index', 'waitlist_type', 'waitlist_name'
        ], id_type='waitlist_spot'
    )

    del df['waitlist_modal_id']

    return df, game_df, waitlist_df

def create_tables(db_name):
    log.info('Setting up tables')
    create_room_table(db_name)
    create_game_table(db_name)
    create_live_game_table(db_name)
    create_live_waitlist_table(db_name)
    create_tournament_game_table(db_name)
    create_tournament_table(db_name)
    create_tournament_structure_table(db_name)
    create_tournament_payout_table(db_name)


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

def create_live_waitlist_table(db_name):
    with ot.SQLiteHandler(db_name) as sqlh:
        if not sqlh.table_exists('live_waitlists'):
            log.info(
                "live_waitlist table doesn't exist, creating live_waitlist "
                "table"
            )
            sqlh.create_table(
                table='live_waitlists',
                col_type_maps={
                    'waitlist_spot_id': 'text',
                    'live_game_id': 'text',
                    'room_id': 'text',
                    'waitlist_modal_id': 'text',
                    'waitlist_index': 'integer',
                    'waitlist_type': 'text',
                    'waitlist_name': 'text',
                    'job_source': 'text',
                    'job_timestamp_system': 'text',
                    'job_timestamp_utc': 'text',
                }
            )
        else:
            log.info('live_waitlists table already exists, skipping table '
                     'creation')

def create_tournament_game_table(db_name):
    with ot.SQLiteHandler(db_name) as sqlh:
        if not sqlh.table_exists('tournament_games'):
            log.info(
                "tournament_games table doesn't exist, creating "
                "tournament_games table"
            )
            sqlh.create_table(
                table='tournament_games',
                col_type_maps={
                    'tournament_game_id': 'text',
                    'tournament_game_name': 'text',
                    'tournament_game_name_raw': 'text',
                    'job_source': 'text',
                    'job_timestamp_system': 'text',
                    'job_timestamp_utc': 'text',
                }
            )
        else:
            log.info(
                'tournament_games table already exists, skipping table creation'
            )

def create_tournament_table(db_name):
    with ot.SQLiteHandler(db_name) as sqlh:
        if not sqlh.table_exists('tournaments'):
            log.info(
                "tournaments table doesn't exist, creating tournaments table"
            )
            sqlh.create_table(
                table='tournaments',
                col_type_maps={
                    'room_id': 'text',
                    'tournament_game_id': 'text',
                    'tournament_id': 'text',
                    'tournament_structure_id': 'text',

                    'name': 'text',
                    'start_datetime': 'text',
                    'reg_open_datetime': 'text',
                    'reg_close_datetime': 'text',
                    'reg_comments': 'text',
                    'total_buy_in': 'integer',
                    'entry_fee': 'integer',
                    'deductions': 'integer',
                    'starting_chips': 'integer',
                    'starting_blinds': 'text',
                    'starting_small_blind': 'integer',
                    'starting_big_blind': 'integer',
                    'ante_type': 'text',
                    'staff_bonus': 'integer',
                    'staff_bonus_chips': 'integer',
                    'reentry': 'text',
                    'rebuys': 'text',
                    'rebuy_cost': 'text',
                    'rebuy_chips': 'text',
                    'addons': 'text',
                    'addon_cost': 'integer',
                    'addon_chips': 'integer',
                    'bounties': 'text',
                    'guarantee': 'integer',
                    'added_money': 'integer',
                    'level_time_mins': 'integer',
                    'break_length_mins': 'integer',
                    'break_frequency': 'text',

                    'job_source': 'text',
                    'job_timestamp_system': 'text',
                    'job_timestamp_utc': 'text',
                }
            )
        else:
            log.info(
                'tournaments table already exists, skipping table creation'
            )

def create_tournament_structure_table(db_name):
    with ot.SQLiteHandler(db_name) as sqlh:
        if not sqlh.table_exists('tournament_structures'):
            log.info(
                "tournament_structures table doesn't exist, creating "
                "tournament_structures table"
            )
            sqlh.create_table(
                table='tournament_structures',
                col_type_maps={
                    'tournament_structure_id': 'text',
                    'tournament_structure_level_id': 'text',

                    'level_index': 'integer',
                    'level_name': 'text',
                    'level_length_mins': 'integer',
                    'level_small_blind': 'integer',
                    'level_big_blind': 'integer',
                    'level_ante': 'integer',

                    'job_source': 'text',
                    'job_timestamp_system': 'text',
                    'job_timestamp_utc': 'text',
                }
            )
        else:
            log.info(
                'tournament_structures table already exists, skipping table '
                'creation'
            )

def create_tournament_payout_table(db_name):
    with ot.SQLiteHandler(db_name) as sqlh:
        if not sqlh.table_exists('tournament_payouts'):
            log.info(
                "tournament_payouts table doesn't exist, creating "
                "tournament_payouts table"
            )
            sqlh.create_table(
                table='tournament_payouts',
                col_type_maps={
                    'tournament_id': 'text',
                    # 'tournament_payout_id': 'text',
                    'tournament_payout_place_id': 'text',

                    'payout_index': 'integer',
                    'place': 'text',
                    'payout': 'integer',

                    'job_source': 'text',
                    'job_timestamp_system': 'text',
                    'job_timestamp_utc': 'text',
                }
            )
        else:
            log.info(
                'tournament_payouts table already exists, skipping table '
                'creation'
            )

if __name__ == '__main__':
    set_option('display.max_columns', None)
    url = 'https://www.pokeratlas.com/poker-room/prime-social-houston' \
          '/tournaments'
    #url = 'https://www.pokeratlas.com/poker-room/kickapoo-lucky-eagle-eagle
    # -pass/tournaments'
    get_tournament_dfs(url, '123')
