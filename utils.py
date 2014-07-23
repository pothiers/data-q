
def decode_dict(byte_dict):
    str_dict = dict()
    for k,v in byte_dict.items():
        str_dict[k.decode()] = v.decode()
    return str_dict
