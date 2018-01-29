from datetime import datetime


def transfer(src_obj, src_key, dest_obj, dest_key, trans_func=None):
    """
    Transfer a value from a nested object obj1 to a flatten object obj2
    :param src_obj: source object
    :param src_key: source key, if the source object is nested, the source key is dot separated string layer1.layer2...layerN
    :param dest_obj: destination object
    :param dest_key: destination key
    :param trans_func: The function applied upon the transferred value
    :return:
    """
    v = get_nested_value(src_obj, src_key)
    if trans_func:
        try:
            v = trans_func(v)
        except:
            v = {}
    if v:
        dest_obj[dest_key] = v


def get_nested_value(obj, dot_separated_keys):
    """
    Get a value in a nested object
    :param obj: source object
    :param dot_separated_keys: dot separated keys. Example: layer_0_key.layer_1_key.layer_2_key
    :return: obj[layer_0_key][layer_1_key][layer_2_key]  or None if the key is not correct
    """
    layered_keys = dot_separated_keys.split(".")
    v = obj
    try:
        for k in layered_keys:
            v = v[k]
    except:
        v = None
    return v


def extract_time_info(time_label):

    time_label = time_label[:19]

    local_time = datetime.strptime(time_label, '%Y-%m-%dT%H:%M:%S')

    time_info = {
        "year": local_time.year,
        "month": local_time.month,
        "day": local_time.day,
        "weekday": local_time.weekday(),
        "time": local_time.hour + 60 * local_time.minute + 3600 * local_time.second
    }

    return time_info


def get_time_stamp(time_label):
    """

    :param time_label:
    :return:
    """
    if time_label[22] == ":" :
        time_label = time_label[:22] + time_label[23:]
        t = datetime.strptime(time_label, '%Y-%m-%dT%H:%M:%S%z')
        return int(t.timestamp())
    else:
        if time_label[-1] == "Z" and time_label[19] == ".":
            t = datetime.strptime(time_label[:19], '%Y-%m-%dT%H:%M:%S')
            return int(t.timestamp())
    return None



if __name__=="__main__":

    obj = extract_time_info("2015-5-5T20:20:12")
    print(obj)