
from ot.utils.list_utils import flatten_list

from ot.utils.web_utils import simple_get

from ot.utils.poker_utils import (
    standardize_game_name, parse_atlas_update_text
)

from ot.xforms.df_xforms import (
    make_id, add_job_info
)

from ot.handlers.db_handler import SQLiteHandler

from ot.utils.misc_utils import is_empty