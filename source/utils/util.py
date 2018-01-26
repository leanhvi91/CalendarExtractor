from datetime import datetime
from dateutil import tz


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
    layered_keys = src_key.split(".")
    v = src_obj
    try:
        for k in layered_keys:
            v = v[k]
    except:
        v = {}
    if v:
        if trans_func:
            v = trans_func(v)
        dest_obj[dest_key] = v


def get_timestamp():


    # utc = datetime.utcnow()
    utc = datetime.strptime("2018-01-26T20:30:00+0700", '%Y-%m-%dT%H:%M:%S%z')

    print(utc)
    print(utc.timestamp())
    print(utc.weekday())
    print(utc.month)


if __name__=="__main__":
    get_timestamp()