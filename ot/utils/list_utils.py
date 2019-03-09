

def flatten_list(l):
    out_list = []
    for l_item in l:
        if isinstance(l_item, list):
            out_list.extend(l_item)
        else:
            out_list.append(l_item)
    return out_list