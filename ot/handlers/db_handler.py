import sqlite3
import os
from pathlib import Path
import pandas as pd

_DB_PATH = str(Path(__file__).parents[2]) + '\\db\\'

class SQLiteHandler(object):

    def __init__(self, db_name, db_path=_DB_PATH):
        self.db_name = db_name
        self.db_path = db_path
        self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self):
        if not self.conn:
            self.conn = sqlite3.connect(self.db_path + self.db_name)
        return self.conn

    def disconnect(self):
        if self.conn:
            self.conn.close()
        self.conn = None

    def create_table(self, table, col_type_maps):
        conn = self.connect()
        cur = conn.cursor()

        query = 'CREATE TABLE {} ('.format(table)
        query += ', '.join(
            ['{k} {v}'.format(k=k, v=v) for k, v in col_type_maps.items()]
        )
        query += ')'

        cur.execute(query)
        conn.commit()

    def table_exists(self, table):
        conn = self.connect()
        cur = conn.cursor()

        query = """
                SELECT name 
                FROM sqlite_master 
                WHERE type='table' 
                    and name = '{}'
            """.format(table)

        cur.execute(query)
        res = cur.fetchall()
        return bool(res)

    def insert_df(self, df, table):
        conn = self.connect()
        cur = conn.cursor()

        query = 'INSERT INTO {} ('.format(table)
        query += ', '.join(list(df.columns))
        query += ') VALUES ('
        query += '?, ' * df.shape[1]
        query = query[:-2] + ')'

        vals = df.to_records(index=False).tolist()
        cur.executemany(query, vals)
        conn.commit()

    def run_query(self, query):
        conn = self.connect()
        cur = conn.cursor()

        cur.execute(query)
        conn.commit()

    def upsert_df(self, df, table, upsert_col):
        conn = self.connect()
        cur = conn.cursor()

        existing_data_query = """
            SELECT {id_col}
            FROM {table}
        """.format(id_col=upsert_col, table=table)
        existing_data_df = pd.read_sql(existing_data_query, conn)
        update_ids = []
        if not existing_data_df.empty:
            update_ids = df[
                df[upsert_col].isin(existing_data_df[upsert_col].tolist())
            ][upsert_col].tolist()

        if update_ids:
            update_id_tuples = [tuple(i) for i in update_ids]
            delete_query = """
                DELETE FROM {table}
                WHERE {id_col} = ?
            """.format(table=table, id_col=upsert_col)
            cur.executemany(delete_query, update_id_tuples)
            conn.commit()

        self.insert_df(df, table)

    def sql_to_df(self, query):
        conn = self.connect()
        out_df = pd.read_sql(query, conn)
        return out_df


if __name__ == '__main__':
    with SQLiteHandler(db_name='test.db') as h:
        h.create_table('test_table', {'a':'integer', 'b':'integer'})
        # df = DataFrame(data={'a': [1, 2, 3], 'b': [4, 5, 6]})
        # h.import_df(df, 'test_table')
        h.table_exists('test_table')
