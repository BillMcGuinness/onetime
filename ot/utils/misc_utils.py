import hashlib
from pandas import isnull

def hash_object(obj, encoding='latin-1'):

    def _create_hash(_obj):
        if isinstance(_obj, (tuple, list)):
            return str(tuple(_create_hash(i) for i in _obj))
        elif isinstance(_obj, set):
            return str(frozenset(_obj))
        elif isinstance(_obj, dict):
            return str(
                tuple((k, _create_hash(v)) for k, v in sorted(_obj.items()))
            )
        elif callable(_obj):
            return _obj.__name__
        return str(_obj)

    hashed = _create_hash(obj)
    if encoding is None:
        hashed_bytes = bytes(str(hashed))
    else:
        hashed_bytes = bytes(str(hashed), encoding=encoding)
    return hashlib.md5(hashed_bytes).hexdigest()

def is_empty(val):
    if val is None:
        return True
    elif isnull(val):
        return True
    elif val.lower() in ('none', 'nan'):
        return True
    return False