from ot.utils.misc_utils import hash_object
import datetime

def make_id(df, df_cols, id_type):
    out_col = id_type + '_id'

    def _make_id(row):
        col_vals = {}
        for col in df_cols:
            row_val = row[col]
            col_vals[str(row_val).lower()] = 1
        id_val = hash_object(col_vals)
        row[out_col] = id_val
        return row

    return df.apply(_make_id, axis=1)


def add_job_info(df, source):
    df['job_source'] = source
    df['job_timestamp_system'] = str(datetime.datetime.now())
    df['job_timestamp_utc'] = str(datetime.datetime.utcnow())
    return df