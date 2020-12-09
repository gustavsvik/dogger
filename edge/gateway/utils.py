import json
import yaml


def nmea_checksum(nmea_data) :

    nmea_bytearray = bytes(nmea_data, encoding='utf8')
    checksum = 0
    for i in range(0, len(nmea_bytearray)) :
        if nmea_bytearray[i] != 44 :
            checksum = checksum ^ nmea_bytearray[i]
    checksum_hex = hex(checksum)
    return checksum_hex[2:]


def get_channel_range_string(channels) :

    return ';;'.join([str(ch) for ch in channels]) + ';;'


def instance_from_dict(cls, argument_dict) :

    return cls(**argument_dict)


def instance_from_dict_string(cls, dict_string) :

    return instance_from_dict(cls, eval(dict_string))


def instance_from_argument_string(cls, argument_string) :

    return instance_from_dict_string(cls, "dict({})".format(argument_string))


def instance_from_json_string(cls, json_string) :

    json_object = json.loads(json_string)
    return instance_from_dict(cls, dict(json_object))

    
def instance_from_yaml_string(cls, yaml_string) :

    yaml_object = yaml.safe_load(yaml_string)
    return instance_from_dict(cls, dict(yaml_object))
