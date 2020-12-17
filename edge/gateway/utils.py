#

import json
import yaml


def downsample(y, size) :

    y_reshape = y.reshape(size, int(len(y)/size))
    y_downsamp = y_reshape.mean(axis=1)
    return y_downsamp


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
