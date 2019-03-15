import unittest
from unittest.mock import Mock, patch, MagicMock
from pandas import DataFrame, set_option
from pandas.util.testing import assert_frame_equal
from datetime import datetime

from ot.xforms import df_xforms


class DfXformsTest(unittest.TestCase):

    def setUp(self):
        set_option('display.max_columns', None)

        self.hash_object_patcher = patch('ot.xforms.df_xforms.hash_object')
        self.MockHashObject = self.hash_object_patcher.start()
        self.addCleanup(self.hash_object_patcher.stop)

        self.datetime_patcher = patch(
            'ot.utils.poker_utils.datetime.datetime'
        )
        self.MockDatetime = self.datetime_patcher.start()
        self.MockDatetime.now.return_value = datetime(
            2019, 3, 13, 9, 00, 0, 1
        )
        self.MockDatetime.utcnow.return_value = datetime(
            2019, 3, 13, 9, 00, 0, 1
        )
        self.addCleanup(self.datetime_patcher.stop)

    def test_make_id(self):
        mock_id_val = 'id_123'
        self.MockHashObject.return_value = mock_id_val

        inp_df = DataFrame(data={
            'col1': ['abc', 'def', 'xyz'],
            'col2': ['123', '456', '789']
        })

        got_df = df_xforms.make_id(
            inp_df, df_cols=['col1', 'col2'], id_type='test'
        )

        exp_df = inp_df.assign(**{
            'test_id': [mock_id_val] * 3
        })

        assert_frame_equal(got_df, exp_df)

    def test_add_job_info(self):
        inp_df = DataFrame(data={
            'col1': ['abc', 'def', 'xyz'],
            'col2': ['123', '456', '789']
        })

        got_df = df_xforms.add_job_info(inp_df, __file__)

        exp_df = inp_df.assign(**{
            'job_source': [__file__] * 3,
            'job_timestamp_system': ['2019-03-13 09:00:00.000001'] * 3,
            'job_timestamp_utc': ['2019-03-13 09:00:00.000001'] * 3,
        })

        assert_frame_equal(got_df, exp_df)