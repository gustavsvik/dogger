#

import json

try : import yaml
except ImportError: pass



def safe_str(obj = None, default_value = None) :
    value = default_value
    if obj is not None :
        try :
            value = str(obj)
        except UnicodeEncodeError as e :
            pass
    return value


def safe_append(string = None, appendix = None, default_value = None) :
    value = default_value
    if not None in [string, appendix] :
        try :
            value = str(string) + str(appendix)
        except UnicodeEncodeError as e :
            pass
    return value


def safe_get(dict_like_obj = None, keys = None, default_value = None) :

    values = default_value
    if dict_like_obj is not None :
        if (type(keys) is list) :
            values = []
            for key in keys :
                value = None
                try :
                    value = dict_like_obj[key]
                except (KeyError, TypeError) as e :
                    pass
                values.append(value)
        else :
            try :
                values = dict_like_obj[keys]
            except (KeyError, TypeError) as e :
                pass

    return values


def safe_index(list_like_obj = None, values = None, non_exist_index = None) :

    index = non_exist_index
    if list_like_obj is not None :
        indices = []
        if (type(values) is list) :
            #common_values = list( set(value).intersection(set(list_like_obj)) )
            #index = common_values  #[0]
            #index = [ list_like_obj.index(value) for value in values ]
            for value in values :
                value_indices = [i for i,item in enumerate(list_like_obj) if item == value]
                indices.extend(value_indices)
        elif values in list_like_obj :
            indices = list_like_obj.index(values)

    return indices


def safe_list(iterable_or_single_object = None) :

    return_list = None

    if iterable_or_single_object is not None :

        if type(iterable_or_single_object) in [bytes, str] :
            return_list = [ iterable_or_single_object ]
        else :
            try :
                return_list = list(iterable_or_single_object)
            except TypeError :
                return_list = [ iterable_or_single_object ]

    return return_list


def instance_from_dict(obj, argument_dict) :

    return obj(**argument_dict)


def instance_from_dict_string(obj, dict_string) :

    return instance_from_dict(obj, eval(dict_string))


def instance_from_argument_string(obj, argument_string) :

    return instance_from_dict_string(obj, "dict({})".format(argument_string))


def instance_from_json_string(obj, json_string) :

    json_object = json.loads(json_string)
    return instance_from_dict(obj, dict(json_object))


def instance_from_yaml_string(obj, yaml_string) :

    yaml_object = yaml.safe_load(yaml_string)
    return instance_from_dict(obj, dict(yaml_object))
