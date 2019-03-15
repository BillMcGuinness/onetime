from ot.utils.misc_utils import is_empty
from pathlib import Path
import pandas as pd
import datetime
import logging
import re

log = logging.getLogger()

def standardize_game_name(game_name):
    game_name = game_name.upper()
    game_name = standardize_variant(game_name)
    game_name = standardize_stakes(game_name)
    return game_name

def standardize_stakes(game_name):

    game_name = game_name.replace('$', '')
    game_name = re.sub('(\d)\/(\d)', r'\1-\2', game_name)
    #game_name = game_name.replace('/', '-')
    return game_name


def standardize_variant(game_name):
    variant_std_df = pd.read_excel(
        str(Path(__file__).parents[0])
        + '\\static\\variant_standardization.xlsx',
        sheet_name='Sheet1',
        header=None
    )
    # print('orig game name: {}'.format(game_name))
    for idx, row in variant_std_df.iterrows():
        good_val = row[0]
        #print('good val: {}'.format(good_val))
        for idx, bad_val in row.iteritems():
            if not is_empty(bad_val):
                #print('bad_val: {}'.format(bad_val))
                game_name = game_name.replace(bad_val, good_val)
                #print('new game name: {}'.format(game_name))

    #print('final game name: {}'.format(game_name))
    return game_name

def parse_atlas_update_text(update_text):
    now_time = datetime.datetime.now()

    update_text_raw = update_text

    update_text = update_text.replace('Last updated:', '')
    update_text = update_text.replace('about', '')
    update_text = update_text.strip()

    if 'minute' in update_text:
        if update_text == 'less than a minute ago':
            return now_time
        else:
            update_text = update_text.replace('minutes ago', '').strip()
            update_text = update_text.replace('minute ago', '').strip()
            try:
                update_time_diff = int(update_text)
            except:
                log.warning(
                    'Cannot parse update text: {}'.format(update_text_raw)
                )
                return update_text_raw
            game_update_time = now_time - datetime.timedelta(
                minutes=update_time_diff
            )
    elif 'hour' in update_text:
        update_text = update_text.replace('hours ago', '').strip()
        update_text = update_text.replace('hour ago', '').strip()
        try:
            update_time_diff = int(update_text)
        except:
            log.warning(
                'Cannot parse update text: {}'.format(update_text_raw)
            )
            return update_text_raw
        game_update_time = now_time - datetime.timedelta(
            hours=update_time_diff
        )
    elif 'day' in update_text:
        update_text = update_text.replace('days ago', '').strip()
        update_text = update_text.replace('day ago', '').strip()
        try:
            update_time_diff = int(update_text)
        except:
            log.warning(
                'Cannot parse update text: {}'.format(update_text_raw)
            )
            return update_text_raw
        game_update_time = now_time - datetime.timedelta(
            days=update_time_diff
        )

    game_update_time_str = str(game_update_time)
    if '.' in game_update_time_str:
        dot_idx = game_update_time_str.find('.')
        str_len = len(game_update_time_str)
        microsecond_from_right = str_len-dot_idx
        game_update_time_str = game_update_time_str[:-microsecond_from_right]

    return game_update_time_str
            


if __name__ == '__main__':
    print(str(Path(__file__).parents[0]) + '\\static\\variant_standardization.xlsx')