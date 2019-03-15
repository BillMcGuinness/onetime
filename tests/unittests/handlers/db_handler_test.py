import unittest
from unittest.mock import Mock, patch, MagicMock
from pandas import DataFrame, set_option
from pandas.util.testing import assert_frame_equal
from collections import OrderedDict

from ot.handlers import db_handler

class SQLiteHandlerTest(unittest.TestCase):

    def setUp(self):
        set_option('display.max_columns', None)

        self.sqlite_connect_patcher = patch(
            'ot.handlers.db_handler.sqlite3.connect'
        )
        self.MockSQLiteConnect = self.sqlite_connect_patcher.start()
        self.addCleanup(self.sqlite_connect_patcher.stop)

        self.pandas_read_sql_patcher = patch(
            'ot.handlers.db_handler.pd.read_sql'
        )
        self.MockReadSql = self.pandas_read_sql_patcher.start()
        self.addCleanup(self.pandas_read_sql_patcher.stop)


    def test_insert_df(self):
        fake_query_results = ['Value1', 'Value2', 'Value3']

        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = fake_query_results
        mock_cursor.executemany.return_value = fake_query_results
        mock_cursor.close.return_value = True

        mock_conn = MagicMock()
        mock_conn.close.return_value = True
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.commit.return_value = True

        self.MockSQLiteConnect.return_value = mock_conn

        ins_df = DataFrame(data={
            'col1': ['abc', 'def', 'xyz'],
            'col2': ['123', '456', '789']
        })

        with db_handler.SQLiteHandler('TEST') as s:
            s.insert_df(ins_df, 'test_table')

            mock_cursor.executemany.assert_called_once_with(
                'INSERT INTO test_table (col1, col2) VALUES (?, ?)',
                [('abc', '123'), ('def', '456'), ('xyz', '789')]
            )

    def test_create_table(self):
        fake_query_results = ['Value1', 'Value2', 'Value3']

        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = fake_query_results
        mock_cursor.executemany.return_value = fake_query_results
        mock_cursor.close.return_value = True

        mock_conn = MagicMock()
        mock_conn.close.return_value = True
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.commit.return_value = True

        self.MockSQLiteConnect.return_value = mock_conn

        with db_handler.SQLiteHandler('TEST') as s:
            s.create_table(
                'test_table',
                OrderedDict([('col1', 'text'), ('col2', 'integer')])
            )

            mock_cursor.execute.assert_called_once_with(
                'CREATE TABLE test_table (col1 text, col2 integer)'
            )

    def test_table_exists(self):
        fake_query_results = ['Value1', 'Value2', 'Value3']

        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = fake_query_results
        mock_cursor.executemany.return_value = fake_query_results
        mock_cursor.fetchall.return_value = [('rooms',)]
        mock_cursor.close.return_value = True

        mock_conn = MagicMock()
        mock_conn.close.return_value = True
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.commit.return_value = True

        self.MockSQLiteConnect.return_value = mock_conn

        with db_handler.SQLiteHandler('TEST') as s:
            got_ans = s.table_exists('rooms')

            mock_cursor.execute.assert_called_once_with(
                """
                SELECT name 
                FROM sqlite_master 
                WHERE type='table' 
                    and name = 'rooms'
            """
            )
            self.assertTrue(got_ans)

        mock_cursor.execute.reset_mock()
        mock_cursor.fetchall.return_value = []

        with db_handler.SQLiteHandler('TEST') as s:
            got_ans = s.table_exists('rooms123')

            mock_cursor.execute.assert_called_once_with(
                """
                SELECT name 
                FROM sqlite_master 
                WHERE type='table' 
                    and name = 'rooms123'
            """
            )
            self.assertFalse(got_ans)

    def test_upsert_df(self):
        fake_query_results = ['Value1', 'Value2', 'Value3']

        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = fake_query_results
        mock_cursor.executemany.return_value = fake_query_results
        mock_cursor.close.return_value = True

        mock_conn = MagicMock()
        mock_conn.close.return_value = True
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.commit.return_value = True

        self.MockSQLiteConnect.return_value = mock_conn

        upsert_col= 'id'
        mock_df = DataFrame(data={
            upsert_col: ['1','2']
        })
        self.MockReadSql.return_value = mock_df

        upsert_df = DataFrame(data={
            upsert_col: ['1','3']
        })

        with db_handler.SQLiteHandler('TEST') as s:
            s.upsert_df(upsert_df, 'test_table', upsert_col)

            self.MockReadSql.assert_called_once_with(
                """
            SELECT id
            FROM test_table
        """, mock_conn
            )

            mock_cursor.executemany.assert_any_call(
                """
                DELETE FROM test_table
                WHERE id = ?
            """, [('1',)]
            )
