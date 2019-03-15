import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

from ot.utils import poker_utils

class PokerUtilsTest(unittest.TestCase):

    def setUp(self):
        self.datetime_patcher = patch(
            'ot.utils.poker_utils.datetime.datetime'
        )
        self.MockDatetime = self.datetime_patcher.start()
        self.MockDatetime.now.return_value = datetime(
            2019, 3, 13, 9, 00, 0, 1
        )
        self.addCleanup(self.datetime_patcher.stop)

    def test_parse_atlas_update_text(self):
        for update_text, exp_text in [
            ('Last updated: about 22 hours ago', '2019-03-12 11:00:00'),
            ('Last updated: about 2 hours ago', '2019-03-13 07:00:00'),
            ('Last updated: about 1 hour ago', '2019-03-13 08:00:00'),
            ('Last updated: 1 minute ago', '2019-03-13 08:59:00'),
            ('Last updated: 39 minutes ago', '2019-03-13 08:21:00'),
            ('Last updated: 1 day ago', '2019-03-12 09:00:00'),

        ]:
            got_text = poker_utils.parse_atlas_update_text(update_text)
            self.assertEqual(exp_text, got_text)

    def test_standardize_variant(self):
        for raw_variant, exp_variant in [
            ('$1/3 NLH', '$1/3 NLH'),
            ('$2/4 LHE', '$2/4 LHE'),
            ('$1/3 PLO', '$1/3 PLO'),
            ('$1/3 ROE', '$1/3 ROE'),
            ('1/3PLO-H/LOW', '1/3PLO-H/L'),
            ('$2-5 NL Holdem', '$2-5 NLH'),
            ('3-6 Limit Holdem', '3-6 LHE'),
            ('1-2 N/L Holdem', '1-2 NLH'),
            ('$15-30 8OB', '$15-30 8OB')
        ]:
            got_variant = poker_utils.standardize_variant(raw_variant)
            self.assertEqual(exp_variant, got_variant)

    def test_standardize_stakes(self):
        for raw_stakes, exp_stakes in [
            ('$1/3 NLH', '1-3 NLH'),
            ('$2/4 LHE', '2-4 LHE'),
            ('$1/3 PLO', '1-3 PLO'),
            ('$1/3 ROE', '1-3 ROE'),
            ('1/3PLO-H/LOW', '1-3PLO-H/LOW'),
            ('$2-5 NL Holdem', '2-5 NL Holdem'),
            ('3-6 Limit Holdem', '3-6 Limit Holdem'),
            ('1-2 N/L Holdem', '1-2 N/L Holdem'),
            ('$15-30 8OB', '15-30 8OB')
        ]:
            got_stakes = poker_utils.standardize_stakes(raw_stakes)
            self.assertEqual(exp_stakes, got_stakes)

    def test_standardize_game(self):
        for raw_variant, exp_variant in [
            ('$1/3 NLH', '1-3 NLH'),
            ('$2/4 LHE', '2-4 LHE'),
            ('$1/3 PLO', '1-3 PLO'),
            ('$1/3 ROE', '1-3 ROE'),
            ('1/3PLO-H/LOW', '1-3PLO-H/L'),
            ('$2-5 NL Holdem', '2-5 NLH'),
            ('3-6 Limit Holdem', '3-6 LHE'),
            ('1-2 N/L Holdem', '1-2 NLH'),
            ('$15-30 8OB', '15-30 8OB')
        ]:
            got_variant = poker_utils.standardize_game_name(raw_variant)
            self.assertEqual(exp_variant, got_variant)

if __name__ == '__main__':
    unittest.main()