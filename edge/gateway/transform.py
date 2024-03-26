#

import time
import datetime
import struct
import math
import numpy
import tempfile
import os
import enum
import json
import re
import binascii

try : import openlocationcode.openlocationcode
except ImportError : pass
try : import bitstring
except ImportError : pass
try : import bitarray
except ImportError : pass
try : import pyais
except ImportError : pass
try : import geohash2
except ImportError : pass

import gateway.runtime as rt
import gateway.utils as ut
import gateway.aislib as ai



def to_json(nested_struct) :
    # TODO: Add checks and balances
    json_string = ''
    json_string = json.dumps(nested_struct)
    return json_string


def from_json(json_string) :
    # TODO: Add checks and balances
    dataset = {}
    try :
        rt.logging.debug("json_string", json_string)
        dataset = json.loads(json_string)
    except json.decoder.JSONDecodeError as e :
        rt.logging.exception(e)
    return dataset


def number_convertible(value) :
    # As per https://stackoverflow.com/a/354038
    return str(value).lstrip('-').replace('.','',1).replace('e-','',1).replace('e','',1).isdigit()


def hex_characters(value) :

    return (ch in string.hexdigits for ch in str(value) )


def strip_alpha_from_string(alpha_num_string) :

    return re.sub("\D", "", alpha_num_string)


def locations_of_substring(string, substring) :
    """Return a list of locations of a substring. ( https://stackoverflow.com/a/19720214 )"""
    substring_length = len(substring)
    def recurse(locations_found, start) :
        location = string.find(substring, start)
        if location != -1:
            return recurse(locations_found + [location], location+substring_length)
        else:
            return locations_found
    return recurse([], 0)


def dict_from_lists(keys_list, values_list) :

    key_value_tuple_list = list(zip(keys_list, values_list))
    rt.logging.debug("key_value_tuple_list", key_value_tuple_list)
    min_common_length = min( len(keys_list), len(values_list) )
    rt.logging.debug("min_common_length", min_common_length)
    data_dict = dict(key_value_tuple_list)
    rt.logging.debug("data_dict", data_dict)
    return data_dict


def list_replace(lst, old, new):
    """replace list elements (inplace), https://stackoverflow.com/a/59478892"""
    i = -1
    try:
        while True:
            i = lst.index(old, i + 1)
            lst[i] = new
    except ValueError:
        pass


#def string_from_lines(data_lines, values_list) :
#
#    first_line_break_index = data_lines.find('\n')
#    data_lines_list_no_header = []
#            if first_line_break_index > 0 :
#                data_lines_list_no_header.append( data_lines[first_line_break_index + 1 : ] )
#            rt.logging.debug("data_lines_list_no_header", data_lines_list_no_header)


def get_channel_range_string(channels) :

    channel_list = ut.safe_list(channels)
    return ';'.join([str(ch) for ch in channel_list]) + ';'


def parse_delimited_string(data_string) :

    channel_data = [channel_string.split(',') for channel_string in data_string.split(';')]
    channel_list = channel_data[0::2][:-1]
    data_list = channel_data[1::2]
    rt.logging.debug("data_list", data_list)
    timestamp_list = [data[0::4][:-1] for data in data_list]
    values_list = [data[1::4] for data in data_list]
    byte_string_list = [data[3::4] for data in data_list]
    rt.logging.debug("byte_string_list", byte_string_list)
    return channel_list, timestamp_list, values_list, byte_string_list


def delimited_string_from_lists(channels, timestamps, values, byte_strings) :

    rt.logging.debug("channels", channels, "timestamps", timestamps, "values", values, "byte_strings", byte_strings)
    data_string = ""
    for channel, timestamp, value, byte_string in zip(ut.safe_list(channels), ut.safe_list(timestamps), ut.safe_list(values), ut.safe_list(byte_strings) ) :
        data_string += str(channel) + ';' + str(timestamp) + ',' + str(value) + ',,' + byte_string.decode() + ',;'

    return data_string


def parse_channel_timestamp_string(data_string) :

    channel_list = []
    timestamp_list = []
    if data_string is not None:
        channel_timestamps = [channel_string.split(',') for channel_string in data_string.split(';')]
        channel_list = channel_timestamps[0::2][:-1]
        timestamp_list = channel_timestamps[1::2]
    return channel_list, timestamp_list


def downsample(y, size) :

    y_reshape = y.reshape(size, int(len(y)/size))
    y_downsamp = y_reshape.mean(axis=1)
    return y_downsamp


def timestamp_to_date_times(timestamp = None, sample_rate = None) :

    if timestamp is None : timestamp = time.time()
    if sample_rate is None : sample_rate = 1.0

    divisor = numpy.int64(1/numpy.float64(sample_rate))

    current_time = numpy.float64(timestamp)
    current_secs = numpy.int64(current_time)
    current_microsecs = numpy.int64(current_time * 1e6)
    current_microsec_part = current_microsecs - numpy.int64(current_secs)*1e6

    next_sample_secs = current_secs + numpy.int64( divisor - current_secs % divisor )

    datetime_current = datetime.datetime.fromtimestamp(current_secs)
    current_timetuple = datetime_current.timetuple()

    return current_secs, current_timetuple, current_microsec_part, next_sample_secs


def replace_byte_chars(byte_string, replace_list = None) :

    for replace_chars in replace_list :
        byte_string = byte_string.replace(replace_chars[0], replace_chars[1])
    return byte_string


def armor_separators_csv(byte_string, replace_list = None) :

    replaced_byte_string = b''
    if replace_list is None :
        replaced_byte_string = byte_string.replace(b',', b'|').replace(b';', b'~')
    else :
        replaced_byte_string = replace_byte_chars(byte_string, replace_list)
    return replaced_byte_string


def de_armor_separators_csv(byte_string, replace_list = None) :

    replaced_byte_string = b''
    if replace_list is None :
        replaced_byte_string = byte_string.replace('|', ',').replace('~', ';')
    else :
        replaced_byte_string = replace_byte_chars(byte_string, replace_list)
    return replaced_byte_string


def dict_from_csv(key_list, csv_string, separator) :

    csv_fields = csv_string.split(separator)
    csv_fields_no_quotes = [csv_field.replace('"', '') for csv_field in csv_fields]
    csv_tuples = dict_from_lists(key_list, csv_fields_no_quotes) #dict(zip(key_list, csv_fields))

    return csv_tuples


def data_field_binary_string(message_data_binary_string, start_in_complete_message, field_start, field_end) :

    message_data_start_index = field_start - start_in_complete_message
    message_data_end_index = field_end - start_in_complete_message + 1
    field_binary_string = message_data_binary_string[ message_data_start_index : message_data_end_index ]
    return field_binary_string


def twos_comp(val, bits):

    """compute the 2's complement of int value val"""
    if (val & (1 << (bits - 1))) != 0: # if sign bit is set e.g., 8bit: 128-255
        val = val - (1 << bits)        # compute negative value
    return val                         # return positive value as is


def get_open_location_code(lat = None, lon = None, length = None) :

    olc = None
    if not None in [lat, lon] and number_convertible(lat) and number_convertible(lon) :
        olc = openlocationcode.openlocationcode.encode(latitude = float(lat), longitude = float(lon), codeLength = 11)

    olc_trimmed = olc
    if olc is not None :
        if length is None :
            length = len(olc)
        olc_trimmed = olc[0:length]
    rt.logging.debug("olc_trimmed", olc_trimmed)
    return olc_trimmed


def get_geohash(lat = None, lon = None, length = None) :

    geohash = geohash2.encode(latitude = float(lat), longitude = float(lon), precision = length)
    rt.logging.debug("geohash.upper()", geohash.upper())
    return geohash.upper()


class Callbacks :


    def get_name_plus_extension(name = None, name_extension = None) :

        name_plus_extension = None
        if name is not None :
            name_plus_extension = name
        if name_extension is not None :
            name_plus_extension += name_extension

        return name_plus_extension



class AisCallbacks(Callbacks) :


    def get_open_location_code(lat = None, lon = None) :

        #return get_open_location_code(lat = lat, lon = lon, length = 7)
        return get_geohash(lat = lat, lon = lon, length = 6)


    def get_geohash(lat = None, lon = None) :

        return get_geohash(lat = lat, lon = lon, length = 6)


    def create_key(mmsi = None, part_2 = None, part_3 = None) :

        composite_key = ut.safe_str(mmsi)
        #composite_key = ut.safe_append(composite_key, ut.safe_append("-", ut.safe_str(part_2)))
        #composite_key = ut.safe_append(composite_key, ut.safe_append("-", ut.safe_str(part_3))

        return composite_key


    def create_location_key(mmsi = None, olc = None, part_3 = None) :

        composite_key = ut.safe_str(mmsi)
        composite_key = ut.safe_append(composite_key, ut.safe_append("-", ut.safe_str(olc)))
        #composite_key = ut.safe_append(composite_key, ut.safe_append("-", ut.safe_str(part_3))
        rt.logging.debug("composite_key (create_location_key)", composite_key)
        return composite_key


    def create_location_message_key(mmsi = None, fid = None, olc = None) :

        composite_key = ut.safe_str(mmsi)
        composite_key = ut.safe_append(composite_key, ut.safe_append("-", ut.safe_str(fid)))
        composite_key = ut.safe_append(composite_key, ut.safe_append("-", ut.safe_str(olc)))
        rt.logging.debug("composite_key (create_location_message_key)", composite_key)
        return composite_key


    def get_host_hardware_id(imo = None, shipname = None, callsign = None, mmsi = None) :

        host_hardware_id = imo
        if host_hardware_id is None :
            if shipname is not None :
                host_hardware_id = shipname

        if imo is None and shipname is None and callsign is None :
            host_hardware_id = mmsi

        return host_hardware_id


    def get_png_name_by_key(mmsi = None, part_2 = None, part_3 = None) :

        png_name = ut.safe_str(mmsi)
        #png_name = ut.safe_append(png_name, ut.safe_append("-", ut.safe_str(part_2)))
        #png_name = ut.safe_append(png_name, ut.safe_append("-", ut.safe_str(part_3)))
        png_name = ut.safe_append(png_name, ".png")

        return png_name


    def get_png_name_by_aton_type(aid_type = None, virtual_aid = None, part_3 = None) :

        png_name = ut.safe_str(aid_type)
        png_name = ut.safe_append(png_name, ut.safe_append("-", ut.safe_str(virtual_aid)))
        #png_name = ut.safe_append(png_name, ut.safe_append("-", ut.safe_str(part_3)))
        png_name = ut.safe_append(png_name, ".png")

        return png_name



class ModbusCallbacks(Callbacks) :


    def average_line_current(IL1 = None, IL2 = None, IL3 = None) :

        IL = ( ut.safe_float(IL1) + ut.safe_float(IL2) + ut.safe_float(IL3) ) / 3

        return IL


    def line_current_sign_from_active_power(IL = None, P = None) :

        abs_to_give_sign = abs( ut.safe_float(IL) )
        signed_operand = abs_to_give_sign * ut.safe_sign(P)

        return signed_operand


    def apparent_power_sign_from_active_power(S = None, P = None) :

        abs_to_give_sign = abs( ut.safe_float(S) )
        signed_operand = abs_to_give_sign * ut.safe_sign(P)

        return signed_operand



class TextNumeric :


    def __init__(self) :

        pass
        #self.prepend = prepend
        #self.append = append
        #if self.prepend is None : self.prepend = ''
        #if self.append is None : self.append = ''


    def append_struct_function_results(self, dataset, message_format, callback_class) :

        rt.logging.debug("dataset", dataset)
        result_dataset = {}
        function_items = { structure_tag : ut.safe_get(structure_item, 'function') for structure_tag, structure_item in message_format.items() if ut.safe_get(structure_item, 'function') is not None }
        for function_member, function_struct in function_items.items() :
            rt.logging.debug("function_member", function_member, "function_struct", function_struct)
            struct_name = ut.safe_get(function_struct, 'name')
            struct_args = ut.safe_get(function_struct, 'args')
            rt.logging.debug("struct_name", struct_name, "struct_args", struct_args)
            if struct_name is not None and struct_args is not None :
                args = { arg : ut.safe_get(dataset, arg) for arg in struct_args }
                rt.logging.debug("args", args)
                func = getattr(callback_class, struct_name)
                rt.logging.debug("func", func)
                return_val = ut.instance_from_dict(func, args)
                rt.logging.debug("return_val", return_val)
                function_eval_data = {function_member:return_val}
                rt.logging.debug("function_eval_data", function_eval_data)
                dataset |= function_eval_data
        rt.logging.debug("dataset", dataset)


    def data_dict_from_struct(self, message_string, message_structure) :

        return_dict = dict()

        bit_offset = None

        for structure_tag, structure_item in message_structure.items() :

            if structure_tag in ['message'] :
                bit_offset = ut.safe_get(structure_item, 'start_pos')

            if structure_tag not in ['message'] :

                bits_arg = ut.safe_get(structure_item, 'bits')

                type_arg = ut.safe_get(structure_item, 'type')
                if type_arg is not None : type_arg = str(type_arg)

                add_arg = ut.safe_get(structure_item, 'add')
                if add_arg is not None : add_arg = float(add_arg)
                div_arg = ut.safe_get(structure_item, 'div')
                if div_arg is not None : div_arg = float(div_arg)
                round_arg = ut.safe_get(structure_item, 'round')
                if round_arg is not None : round_arg = int(round_arg)

                underflow_arg = ut.safe_get(structure_item, 'underflow')
                if underflow_arg is not None : underflow_arg = int(underflow_arg)
                overflow_arg = ut.safe_get(structure_item, 'overflow')
                if overflow_arg is not None : overflow_arg = int(overflow_arg)
                #overflow_var_flag_pos = ut.safe_get(structure_item, 'overflow_var_flag')
                #overflow_var_flag_arg = None

                nosensor_arg = ut.safe_get(structure_item, 'nosensor')
                if nosensor_arg is not None : nosensor_arg = int(nosensor_arg)

                novalue_arg = ut.safe_get(structure_item, 'novalue')
                values_arg = ut.safe_get(structure_item, 'values')
                function_arg = ut.safe_get(structure_item, 'function')

                return_val = None

                if bits_arg is not None and not None in bits_arg :
                    if isinstance(message_string, bytes) : message_string = ''.join([format(the_byte, '08b') for the_byte in message_string])
                    return_val = self.data_dict_from_packed_sequence(message_string, structure_item, bit_offset, bits_arg, type_arg)
                    rt.logging.debug("return_val", return_val)

                if return_val is None :
                    return_val = ut.safe_get(message_string, structure_tag)
                    if isinstance(return_val, enum.Enum) :
                        return_val = return_val.value

                if return_val is not None and number_convertible(return_val) :
                    int_return_val = None
                    try :
                        int_return_val = int(return_val)
                    except ValueError :
                        int_return_val = int(round(float(return_val)))
                    if underflow_arg is not None and int_return_val == underflow_arg :
                        return_val = float('-inf')
                    elif ( overflow_arg is not None and int_return_val == overflow_arg ) : #or ( overflow_var_flag_arg is not None and overflow_var_flag_arg == True ) :
                        return_val = float('inf')
                    if nosensor_arg is not None and int_return_val == nosensor_arg :
                        return_val = None
                    elif novalue_arg is not None :
                        if ( type(novalue_arg) is list and int_return_val in novalue_arg ) or int_return_val == novalue_arg :
                            return_val = None

                if div_arg is not None or add_arg is not None :
                    if return_val is not None and number_convertible(return_val) :
                        if type(return_val) == str :
                            return_val = float(return_val)
                        if div_arg is not None :
                            return_val /= div_arg
                            if type_arg[0] in ['U','I'] and div_arg < 1.0 :
                                try :
                                    return_val = int(return_val)
                                except ValueError :
                                    return_val = int(round(float(return_val)))
                        if add_arg is not None :
                            return_val += add_arg

                if return_val is not None and number_convertible(return_val) and type_arg is not None and ( type_arg in ['int', 'float'] or type_arg[0] in ['U','I','f'] ) :

                    if round_arg is not None :
                        return_val = round(float(return_val), round_arg)

                    if type_arg == 'int' :
                        return_val = int(round(float(return_val)))
                    elif type_arg == 'float' :
                        return_val = float(return_val)

                if return_val is not None and type_arg is not None and type_arg not in ['bool'] and type_arg[0] in ['b'] :

                    return_val = bool(return_val)

#                if return_val is None and function_arg is not None :
#
#                    struct_name = ut.safe_get(function_arg, 'name')
#                    struct_args = ut.safe_get(function_arg, 'args')
#                    if struct_name is not None and struct_args is not None :
#                        args = { arg : ut.safe_get(return_dict, arg) for arg in struct_args }
#                        func = getattr(Callbacks, struct_name)
#                        return_val = ut.instance_from_dict(func, args)

                if values_arg is None or ( values_arg is not None and return_val in values_arg ) :
                    rt.logging.debug("structure_tag", structure_tag, "return_val", return_val)
                    rt.logging.debug(" ")
                    return_dict[structure_tag] = return_val

                rt.logging.debug("return_val", return_val)

        rt.logging.debug("return_dict", return_dict)
        return return_dict


    def data_dict_from_packed_sequence(self, message_string, structure_item, bit_offset, bits_arg, type_arg) :

        overflow_var_flag_pos = ut.safe_get(structure_item, 'overflow_var_flag')
        overflow_var_flag_arg = None

        bits_arg_start = bits_arg[0]
        bits_arg_end = bits_arg[1]
        if overflow_var_flag_pos is not None :
            overflow_var_flag_field_string = data_field_binary_string(message_string, bit_offset, overflow_var_flag_pos[0], overflow_var_flag_pos[1])
            overflow_var_flag_arg = bool(overflow_var_flag_field_string)
            bits_arg_start += overflow_var_flag_pos[1] - overflow_var_flag_pos[0] + 1
        rt.logging.debug("message_string", message_string, "bit_offset", bit_offset, "bits_arg_start", bits_arg_start, "bits_arg_end", bits_arg_end)
        return_val = data_field_binary_string(message_string, bit_offset, bits_arg_start, bits_arg_end)
        rt.logging.debug("return_val", return_val)
        if return_val is not None and type_arg is not None and type_arg not in ['int', 'float', 'bool', 'str'] :

            if type_arg[0] in ['u','U','e'] :
                unpacked_int = 9999
                if number_convertible(return_val) :
                    unpacked_int = int(return_val, 2)
                else :
                    no_of_value_registers = len(return_val)
                    return_val.reverse()
                    format_string = "<" + "H" * no_of_value_registers
                    packed_string = struct.pack(format_string, *return_val)
                    unpacked_int = struct.unpack("I", packed_string)[0]
                    rt.logging.debug("unpacked_int", unpacked_int)
                return_val = unpacked_int
            if type_arg[0] in ['i','I'] :
                unpacked_int = -9999
                rt.logging.debug("return_val", return_val)
                if number_convertible(return_val) :
                    unpacked_int = twos_comp( int(return_val, 2), len(return_val) )
                else :
                    no_of_value_registers = len(return_val)
                    return_val.reverse()
                    format_string = "<" + "H" * no_of_value_registers
                    packed_string = struct.pack(format_string, *return_val)
                    unpacked_int = struct.unpack("i", packed_string)[0]
                    rt.logging.debug("unpacked_int", unpacked_int)
                return_val = unpacked_int
            if type_arg[0] in ['t'] :
                return_val = pyais.util.encode_bin_as_ascii6(bitarray.bitarray(return_val))
                rt.logging.debug("return_val (result of encode_bin_as_ascii6)", return_val)
            if type_arg[0] in ['f'] :
                no_of_value_registers = len(return_val)
                unpacked_float = -9999.0
                if no_of_value_registers > 0 :
                    return_val.reverse()
                    format_string = "<" + "H" * no_of_value_registers
                    packed_string = struct.pack(format_string, *return_val)
                    unpacked_float = struct.unpack("f", packed_string)[0]
                    rt.logging.debug("unpacked_float", unpacked_float)
                return_val = unpacked_float

        if overflow_var_flag_arg is not None and overflow_var_flag_arg :
            return_val = float('inf')

        return return_val


    def get_tagged_string_data(self, char_data = None, channel_data = None, line_end = None) :

        string_dict = {}

        for selected_line in char_data :

            try :
                selected_line = selected_line.decode()
            except (UnicodeDecodeError, AttributeError) as e :
                rt.logging.exception(e)
            rt.logging.debug("selected_line", selected_line)
            rt.logging.debug("channel_data.items()", channel_data.items())
            for selected_tag, channels in channel_data.items() :
                selected_tag = str(selected_tag)
                channels_list = list(channels)
                rt.logging.debug("channels_list", channels_list)
                #TODO: Take into account that a single selected_line can contain several separated strings which potentially can be different NMEA/AIS sentences as identified by selected_tag. In current state, loss of data may occur.
                if selected_tag in selected_line :
                    rt.logging.debug("selected_tag", selected_tag)
                    current_string_value = ut.safe_get(string_dict, selected_tag, '')
                    # current_string_value = ''
                    # try :
                    #     current_string_value = string_dict[selected_tag]
                    # except KeyError as e :
                    #     rt.logging.debug(rt.logging.exception)
                    string_dict[selected_tag] = current_string_value + selected_line
                    rt.logging.debug("string_dict", string_dict)
                    if line_end is not None :
                        string_dict[selected_tag] += line_end

        return string_dict


    def decode_to_channels(self, char_data = None, channel_data = None, time_tuple = None, line_end = None) :

        if time_tuple is None :
            time_tuple = time.gmtime(time.time())
            rt.logging.debug("time_tuple", time_tuple)

        string_dict = {}

        if not ( None in [char_data, channel_data] ) :

            # for selected_line in char_data :
            #
            #     try :
            #         selected_line = selected_line.decode()
            #     except (UnicodeDecodeError, AttributeError) :
            #         pass
            #
            #     for selected_tag, channels in channel_data.items() :
            #         selected_tag = str(selected_tag)
            #         rt.logging.debug("selected_tag", selected_tag)
            #         rt.logging.debug("channels", channels)
            #         channels_list = list(channels)
            #         #TODO: Take into account that a single selected_line can contain several separated strings which potentially can be different NMEA/AIS sentences as identified by selected_tag. In current state, loss of data may occur.
            #         if selected_tag in selected_line :
            #             rt.logging.debug("selected_tag", selected_tag, "channels_list", channels_list)
            #             current_string_value = ''
            #             try :
            #                 current_string_value = string_dict[selected_tag]
            #             except KeyError as e :
            #                 pass
            #             string_dict[selected_tag] = current_string_value + selected_line
            #             if line_end is not None :
            #                 string_dict[selected_tag] += line_end

            string_dict = self.get_tagged_string_data(char_data = char_data, channel_data = channel_data, line_end = line_end)
        rt.logging.debug("string_dict", string_dict)

        data_arrays = []

        for selected_tag, channels in channel_data.items() :

            rt.logging.debug("channel_data", channel_data, "selected_tag", selected_tag, "channels", channels)
            channels_list = list(channels) #sorted(list(channels))
            rt.logging.debug("channels_list", channels_list)

            dict_string = ''
            try :
                dict_string = string_dict[selected_tag] #+ '\r\n'
            except KeyError as e :
                pass
            rt.logging.debug("dict_string", dict_string)

            if line_end is not None :
                if dict_string.endswith(line_end) :
                    dict_string = dict_string[:-len(line_end)]

            if len(dict_string) > 0 :

                data_list = [ dict_string ]

                if selected_tag == 'MMB' :
                    value = self.to_float(dict_string, 1)
                    data_list = [ dict_string, [ value ] ]

                if selected_tag == 'ALV' :
                    pass

                if selected_tag == 'ALR' :
                    pass

                if selected_tag == 'TTM' :
                    pass

                if selected_tag == 'GGA' :
                    rt.logging.debug("channels_list", channels_list, "dict_string", dict_string, "time_tuple", time_tuple)
                    validated_timestamp, timestamp_microsecs, latitude, longitude = self.gga_to_time_pos(dict_string, time_tuple)
                    rt.logging.debug("[latitude]", [latitude], "[longitude]", [longitude])
                    if not ( None in [latitude, longitude] ) :
                        gga_string_valid_time = self.gga_from_time_pos_float(validated_timestamp, latitude, longitude)
                        rt.logging.debug("gga_string_valid_time", gga_string_valid_time)
                        data_list = [ gga_string_valid_time, [latitude], [longitude] ]
                    rt.logging.debug("data_list", data_list)

                if selected_tag == 'GLL' :
                    pass

                if selected_tag == 'VTG' :
                    pass

                if selected_tag == 'RSA' :
                    pass

                if selected_tag == 'XDR' :
                    pass

                rt.logging.debug("channels_list", channels_list, "data_list", data_list)
                data_array = [ dict_from_lists( channels_list, data_list ) ]

                rt.logging.debug("data_array", data_array)
                if len(data_array) > 0 :
                    data_arrays.append( dict( [ (selected_tag, data_array) ] ) )

        rt.logging.debug("data_arrays", data_arrays)
        return data_arrays



class Modbus(TextNumeric) :


    def __init__(self) :

        TextNumeric.__init__(self)



class ModbusData(Modbus) :


    def __init__(self, message_formats = None) :

        self.message_formats = message_formats

        Modbus.__init__(self)


    def decode_to_channels(self, char_data = None, channel_data = None, time_tuple = None, line_end = None) :

        if time_tuple is None :
            time_tuple = time.gmtime(time.time())
            rt.logging.debug("time_tuple", time_tuple)

        string_dict = {}
        rt.logging.debug("char_data", char_data, "channel_data", channel_data)
        if not ( None in [char_data, channel_data] ) :
            string_dict = self.get_tagged_string_data(char_data = char_data, channel_data = channel_data, line_end = line_end)
        rt.logging.debug("string_dict", string_dict)

        data_arrays = []

        rt.logging.debug("channel_data", channel_data)
        for selected_tag, channels in channel_data.items() :

            channels_list = list(channels) #sorted(list(channels))
            rt.logging.debug("channels_list", channels_list)

            dict_string = ''
            try :
                dict_string = string_dict[selected_tag] #+ '\r\n'
            except KeyError as e :
                rt.logging.exception(e)
            rt.logging.debug("dict_string", dict_string)

            if len(dict_string) > 0 :

                data_list = [ dict_string ]
                rt.logging.debug("selected_tag", selected_tag)
                if 'SLAVE' in selected_tag :
                    modbus_register_array = from_json( '{' + dict_string + '}' )
                    rt.logging.debug("modbus_register_array", modbus_register_array)
                    modbus_datasets = self.decode_message([ ut.safe_get(modbus_register_array, selected_tag) ], line_end)
                    rt.logging.debug("modbus_datasets", modbus_datasets)
                    modbus_datasets_list = []
                    for modbus_dataset in modbus_datasets :
                        modbus_datasets_list.append( [ None, None, selected_tag , modbus_dataset ] )
                    rt.logging.debug("dict_string", dict_string, "modbus_datasets_list", modbus_datasets_list, "len(modbus_datasets_list)", len(modbus_datasets_list))
                    data_list = [ dict_string, modbus_datasets_list ]

                data_array = [ dict_from_lists( channels_list, data_list ) ]

                if len(data_array) > 0 :
                    data_arrays.append( dict( [ (selected_tag, data_array) ] ) )

        rt.logging.debug("data_arrays", data_arrays)
        return data_arrays


    def decode_message(self, modbus_message_strings, line_end) :

        rt.logging.debug("modbus_message_strings", modbus_message_strings)

        modbus_datasets = []

        for modbus_message_string in modbus_message_strings :
            rt.logging.debug("modbus_message_string", modbus_message_string)
            message_format = {}
            current_format_index = 0
            message_format = ut.safe_get(self.message_formats, current_format_index)
            rt.logging.debug("message_format", message_format)
            modbus_dataset = self.data_dict_from_struct(modbus_message_string, message_format)
            self.append_struct_function_results(modbus_dataset, message_format, ModbusCallbacks)
            rt.logging.debug("modbus_dataset", modbus_dataset)
            modbus_datasets.append(modbus_dataset)

        return modbus_datasets


class NetCdf(TextNumeric) :


    def __init__(self) :

        #self.prepend = prepend
        #self.append = append

        TextNumeric.__init__(self)



class Nmea(TextNumeric) :


    def __init__(self) :

        TextNumeric.__init__(self)


    def get_checksum(self, nmea_data) :

        nmea_bytearray = bytes(nmea_data, encoding='utf8')
        checksum = 0
        for i in range(0, len(nmea_bytearray)) :
            if nmea_bytearray[i] != 44 :
                checksum = checksum ^ nmea_bytearray[i]
        checksum_hex = hex(checksum)
        return checksum_hex[2:]



class NmeaSentence(Nmea) :


    def __init__(self, prepend = None, append = None, message_formats = None) :

        self.prepend = prepend
        self.append = append
        self.message_formats = message_formats

        Nmea.__init__(self)


    def to_float(self, nmea_string, index) :

        nmea_fields = nmea_string.split(',')
        rt.logging.debug("nmea_fields", nmea_fields)

        try :
            value = float(nmea_fields[index])
        except ValueError as e :
            value = -9999.0

        rt.logging.debug("value", value)
        return value


    def from_float(self, multiplier, decimals, value) :

        nmea_string = ''

        try :

            nmea_data = self.prepend
            try :
                nmea_data += "{:.{}f}".format(float(value) * multiplier, decimals)
            except ValueError as e :
                nmea_data += '9999.0'
                rt.logging.exception(e)
            finally :
                nmea_data += self.append
            rt.logging.debug(nmea_data)

            nmea_string = '$' + nmea_data + '*' + self.get_checksum(nmea_data) + '\n'
            rt.logging.debug("nmea_string", nmea_string)

        except Exception as e :

            rt.logging.exception(e)

        finally :

            return nmea_string


    def pos_from_float(self, latitude_float, longitude_float) :

        latitude_string = None
        longitude_string = None

        if not ( None in [latitude_float, longitude_float] ) :

            latitude_dir = 'N'
            latitude_abs = abs(float(latitude_float))
            latitude_sign = float(latitude_float) / latitude_abs
            if latitude_sign < 0 : latitude_dir = 'S'
            latitude_deg = latitude_abs // 1
            latitude_min = ( latitude_abs - latitude_deg ) * 60
            latitude_string = "{:.{}f}".format(latitude_deg * 100 + latitude_min, 7) + ',' + latitude_dir

            longitude_dir = 'E'
            longitude_abs = abs(float(longitude_float))
            longitude_sign = float(longitude_float) / longitude_abs
            if longitude_sign < 0 : longitude_dir = 'W'
            longitude_deg = longitude_abs // 1
            longitude_min = ( longitude_abs - longitude_deg ) * 60

            lead_zero = ''
            if longitude_deg < 100 :
                lead_zero = '0'
                if longitude_deg < 10 :
                    lead_zero = '00'

            longitude_string = lead_zero + "{:.{}f}".format(longitude_deg * 100 + longitude_min, 7) + ',' + longitude_dir

        return latitude_string, longitude_string


    def pos_to_float(self, latitude_string, latitude_cardinal, longitude_string, longitude_cardinal) :

        latitude_deg = float(latitude_string) // 100
        latitude_min = float(latitude_string) - latitude_deg * 100
        latitude = latitude_deg + latitude_min / 60
        if latitude_cardinal == 'S' : latitude = -latitude

        longitude_deg = float(longitude_string) // 100
        longitude_min = float(longitude_string) - longitude_deg * 100
        longitude = longitude_deg + longitude_min / 60
        if longitude_cardinal == 'W' : longitude = -longitude

        return latitude, longitude


    def time_from_timestamp(self, timestamp) :

        datetime_origin = datetime.datetime.fromtimestamp(int(timestamp))
        origin_timetuple = datetime_origin.timetuple()
        time_string = "{:.{}f}".format(origin_timetuple.tm_hour * 10000 + origin_timetuple.tm_min * 100 + origin_timetuple.tm_sec, 2).rjust(9, '0')
        rt.logging.debug("time_string", time_string)
        return time_string


    def time_to_time_members(self, time_string) :

        hour = int(float(time_string)) // 10000
        minute = ( int(float(time_string)) - hour * 10000 ) // 100
        second = int(float(time_string)) - hour * 10000 - minute * 100
        microsec = int ( ( float(time_string) - hour * 10000 - minute * 100 - second ) * 1000000 )

        return hour, minute, second, microsec


    def gll_from_time_pos_float(self, timestamp, latitude, longitude) :

        nmea_data = self.prepend

        try :
            latitude_string, longitude_string = self.pos_from_float(latitude, longitude)
            nmea_data += latitude_string + ',' + longitude_string + ',' + self.time_from_timestamp(timestamp)
        except ValueError as e :
            nmea_data += '9999.0,N,9999.0,E'
            rt.logging.exception(e)
        finally :
            nmea_data += self.append

        nmea_string = '$' + nmea_data + '*' + self.get_checksum(nmea_data) + '\n'
        rt.logging.debug('nmea_string', nmea_string)

        return nmea_string


    def gga_from_time_pos_float(self, timestamp, latitude, longitude) :

        rt.logging.debug('self.prepend', self.prepend)
        nmea_data = self.prepend

        try :
            latitude_string, longitude_string = self.pos_from_float(latitude, longitude)
            nmea_data += self.time_from_timestamp(timestamp) + ',' + latitude_string + ',' + longitude_string
        except ValueError as e :
            nmea_data += '9999.0,N,9999.0,E'
            rt.logging.exception(e)
        finally :
            nmea_data += self.append

        nmea_string = '$' + nmea_data + '*' + self.get_checksum(nmea_data) + '\n'
        rt.logging.debug('nmea_string', nmea_string)

        return nmea_string


    def gga_to_time_pos(self, nmea_string, current_timetuple) :

        rt.logging.debug("nmea_string", nmea_string)
        nmea_fields = nmea_string.split(',')
        rt.logging.debug("nmea_fields", nmea_fields)

        year = current_timetuple[0]
        month = current_timetuple[1]
        monthday = current_timetuple[2]
        hour = current_timetuple[3]
        minute = current_timetuple[4]
        second = current_timetuple[5]
        microsec = current_timetuple[6]

        latitude = None
        longitude = None

        try :
            if len(nmea_fields) > 5 :
                if int(float(nmea_fields[1])) >= 0 and int(float(nmea_fields[1])) <= 235959 :
                    hour, minute, second, microsec = self.time_to_time_members(nmea_fields[1])
                latitude, longitude = self.pos_to_float(nmea_fields[2], nmea_fields[3], nmea_fields[4], nmea_fields[5])
        except ValueError as e :
            rt.logging.exception(e)

        available_datetime = datetime.datetime(year, month, monthday, hour, minute, second, microsec)
        timestamp_secs = int(available_datetime.timestamp())

        return timestamp_secs, microsec, latitude, longitude



class Ais(Nmea) :


    def __init__(self, prepend = None, append = None, message_formats = None) :

        self.prepend = prepend
        self.append = append
        self.message_formats = message_formats

        Nmea.__init__(self)

        self.message_ids = self.get_message_ids(self.message_formats)


    def decode_to_channels(self, char_data = None, channel_data = None, time_tuple = None, line_end = None) :

        rt.logging.debug("char_data", char_data)
        if time_tuple is None :
            time_tuple = time.gmtime(time.time())
            rt.logging.debug("time_tuple", time_tuple)

        string_dict = {}

        if not ( None in [char_data, channel_data] ) :

        #     for selected_line in char_data :
        #
        #         try :
        #             selected_line = selected_line.decode()
        #         except (UnicodeDecodeError, AttributeError) as e :
        #             rt.logging.exception(e)
        #         rt.logging.debug("selected_line", selected_line)
        #         rt.logging.debug("channel_data.items()", channel_data.items())
        #         for selected_tag, channels in channel_data.items() :
        #             selected_tag = str(selected_tag)
        #             rt.logging.debug("selected_tag", selected_tag)
        #             rt.logging.debug("channels", channels)
        #             channels_list = list(channels)
        #             #TODO: Take into account that a single selected_line can contain several separated strings which potentially can be different NMEA/AIS sentences as identified by selected_tag. In current state, loss of data may occur.
        #             if selected_tag in selected_line :
        #                 rt.logging.debug("selected_tag", selected_tag, "channels_list", channels_list)
        #                 current_string_value = ''
        #                 try :
        #                     current_string_value = string_dict[selected_tag]
        #                 except KeyError as e :
        #                     rt.logging.exception(e)
        #                 string_dict[selected_tag] = current_string_value + selected_line
        #                 if line_end is not None :
        #                     string_dict[selected_tag] += line_end

            rt.logging.debug("char_data", char_data, "channel_data", channel_data)
            string_dict = self.get_tagged_string_data(char_data = char_data, channel_data = channel_data, line_end = line_end)

        rt.logging.debug("string_dict", string_dict)

        data_arrays = []

        for selected_tag, channels in channel_data.items() :

            rt.logging.debug("channel_data", channel_data, "selected_tag", selected_tag, "channels", channels)
            channels_list = list(channels) #sorted(list(channels))
            rt.logging.debug("channels_list", channels_list)

            dict_string = ''
            try :
                dict_string = string_dict[selected_tag] #+ '\r\n'
            except KeyError as e :
                rt.logging.exception(e)
            rt.logging.debug("dict_string", dict_string)

            if line_end is not None :
                if dict_string.endswith(line_end) :
                    dict_string = dict_string[:-len(line_end)]
            rt.logging.debug("dict_string", dict_string)

            if len(dict_string) > 0 :

                data_list = [ dict_string ]

                rt.logging.debug("selected_tag", selected_tag)
                if selected_tag == 'VDM' :
                    ais_datasets = self.decode_message([dict_string], line_end)
                    rt.logging.debug("ais_datasets", ais_datasets)

                    ais_datasets_list = []
                    for ais_dataset in ais_datasets :
                        ais_datasets_list.append( [ None, None, ut.safe_str(ut.safe_get(ais_dataset, "mmsi")) , ais_dataset ] )
                    rt.logging.debug("dict_string", dict_string, "ais_datasets_list", ais_datasets_list, "len(ais_datasets_list)", len(ais_datasets_list))
                    data_list = [ dict_string, ais_datasets_list ]

                if selected_tag == 'VDO' :
                    latitude, longitude = self.aivdo_to_pos(dict_string.split(line_end)[0])
                    data_list = [ dict_string, [latitude], [longitude] ]

                if selected_tag == 'AISHUB' :
                    key_list = ['mmsi', 'timestamp', 'lat', 'lon', 'course', 'speed', 'heading', 'status', 'imo', 'shipname', 'callsign', 'shiptype', 'to_bow', 'to_stern', 'to_port', 'to_starboard', 'draught', 'destination', 'eta']
                    csv_dict, ais_datasets = self.aivdm_from_aishub(key_list, dict_string)
                    rt.logging.debug("ais_datasets", ais_datasets)
                    ais_datasets_list = []
                    for ais_dataset in ais_datasets :
                        ais_datasets_list.append( ais_dataset )
                    rt.logging.debug("dict_string", dict_string, "ais_datasets_list", ais_datasets_list, "len(ais_datasets_list)", len(ais_datasets_list))
                    data_list = [ dict_string, ais_datasets[0] ]
                    rt.logging.debug("channels_list", channels_list, "data_list", data_list)

                data_array = [ dict_from_lists( channels_list, data_list ) ]
                rt.logging.debug("data_array", data_array)

                if len(data_array) > 0 :
                    data_arrays.append( dict( [ (selected_tag, data_array) ] ) )

        rt.logging.debug("data_arrays", data_arrays)
        return data_arrays


    def aivdo_to_pos(self, aivdo_string) :

        aivdo_message = ai.AISPositionReportMessage()
        aivdo_instance = ai.AIS(aivdo_message)

        latitude = None
        longitude = None
        try :
            rt.logging.debug("aivdo_string", aivdo_string)
            aivdo_data = aivdo_instance.decode(aivdo_string, ignore_crc = True)
            rt.logging.debug("aivdo_data.mmsi.int", aivdo_data.mmsi.int)
            longitude = aivdo_data.lon.int / 10000 / 60
            latitude = aivdo_data.lat.int / 10000 / 60
        except (KeyError, ValueError, UnboundLocalError) as e :
            rt.logging.exception(e)

        return latitude, latitude


    def aivdm_from_static(self, mmsis, call_signs, vessel_names, ship_types, destinations) :

        aivdm_payloads = []

        try :

            rt.logging.debug("mmsis", mmsis, "call_signs", call_signs, "vessel_names", vessel_names, "ship_types", ship_types, "destinations", destinations)

            for mmsi, call_sign, vessel_name, ship_type, destination in zip(mmsis, call_signs, vessel_names, ship_types, destinations) :

                aivdm_message = ai.AISStaticAndVoyageReportMessage(mmsi = mmsi, imo = 0, callsign = call_sign, shipname = vessel_name, shiptype = ship_type, destination = destination)
                aivdm_instance = ai.AIS(aivdm_message)
                aivdm_payload = aivdm_instance.build_payload(False)
                rt.logging.debug("aivdm_payload", aivdm_payload)

                aivdm_payloads.append(aivdm_payload)

        except(ValueError, bitstring.CreationError) as e :

            rt.logging.exception(e)

        return aivdm_payloads


    def aivdm_from_pos(self, mmsis, speeds, courses, statuses, length_offsets, width_offsets, timestamp, latitude, longitude) :

        aivdm_payloads = []

        try :

            rt.logging.debug("mmsis", mmsis, "speeds", speeds, "courses", courses, "statuses", statuses, "length_offsets", length_offsets, "width_offsets", width_offsets)
            datetime_origin = datetime.datetime.fromtimestamp(int(timestamp))
            origin_timestamp = datetime_origin.timetuple()
            sec = origin_timestamp.tm_sec

            for mmsi, speed, course, status, length_offset, width_offset in zip(mmsis, speeds, courses, statuses, length_offsets, width_offsets) :

                latitude_degs = float(latitude)
                longitude_degs = float(longitude)

                latitude_degs += float ( length_offset / ( 1 * 1852 ) * 1/60 )
                latitude_min_fraction = int(latitude_degs * 60 * 10000)
                rt.logging.debug("latitude_degs", latitude_degs, "latitude_min_fraction", latitude_min_fraction)

                latitude_factor = math.cos(latitude_degs/180 * math.pi)
                longitude_degs += float ( width_offset / ( latitude_factor * 1852 ) * 1/60 )
                longitude_min_fraction = int(longitude_degs * 60 * 10000)
                rt.logging.debug("longitude_degs", longitude_degs, "longitude_min_fraction", longitude_min_fraction)

                speed_deci_kts = int(speed * 10)
                course_deci_degs = int(course * 10)

                aivdm_message = ai.AISPositionReportMessage(mmsi = mmsi, lon = longitude_min_fraction, lat = latitude_min_fraction, sog = speed_deci_kts, cog = course_deci_degs, ts = sec, status = status)
                aivdm_instance = ai.AIS(aivdm_message)
                aivdm_payload = aivdm_instance.build_payload(False)
                rt.logging.debug("aivdm_payload", aivdm_payload)

                aivdm_payloads.append(aivdm_payload)

        except(ValueError, bitstring.CreationError) as e :

            rt.logging.exception(e)

        return aivdm_payloads


    def aivdm_from_aishub(self, key_list, csv_string) :

        aivdm_payloads = []
        aivdm_payload = ''

        csv_string_lines = csv_string.splitlines()[1:]
        rt.logging.debug("csv_string_lines", csv_string_lines)

        aivdm_payload = ''

        for csv_string_line in csv_string_lines :

            try :

                csv_dict = dict_from_csv(key_list, csv_string_line, ',')
                #csv_dict.pop("type_tag", None)

                eta_value = ut.safe_get(csv_dict, 'eta', 0)
                eta_bitstring = None
                if number_convertible(eta_value) :
                    eta_bitstring = '{:0>20}'.format( format(int(eta_value), 'b') )
                month = day = 0
                hour = 24
                minute = 60
                if eta_bitstring is not None :
                    month = int(eta_bitstring[:4], 2)
                    day = int(eta_bitstring[4:9], 2)
                    hour = int(eta_bitstring[9:14], 2)
                    minute = int(eta_bitstring[14:], 2)
                rt.logging.debug("month, day, hour, minute", month, day, hour, minute)
                #csv_dict |= {"month": month, "day": day, "hour": hour, "minute": minute}
                csv_dict.pop("eta", None)
                timestamp_value = ut.safe_get(csv_dict, 'timestamp', 0)
                time_tuple = time.gmtime(int(timestamp_value))
                sec = time_tuple.tm_sec
                rt.logging.debug("csv_dict", csv_dict)

                #aivdm_payload = json.dumps(csv_dict)
                #rt.logging.debug("aivdm_payload", aivdm_payload)

                mmsi_string = str(ut.safe_get(csv_dict, "mmsi", 0))

                mmsi = int(mmsi_string)
                imo = int(ut.safe_get(csv_dict, "imo", 0))
                callsign = ut.safe_get(csv_dict, "callsign", 0)
                shipname = ut.safe_get(csv_dict, "shipname", 0)
                shiptype = int(ut.safe_get(csv_dict, "shiptype", 0))
                to_bow = int(ut.safe_get(csv_dict, "to_bow", 0))
                to_stern = int(ut.safe_get(csv_dict, "to_stern", 0))
                to_port = int(ut.safe_get(csv_dict, "to_port", 0))
                to_starboard = int(ut.safe_get(csv_dict, "to_starboard", 0))
                draught = int(ut.safe_get(csv_dict, "draught", 0))
                destination = ut.safe_get(csv_dict, "destination", 0)
                lon = int(ut.safe_get(csv_dict, "lon", 0))
                lat = int(ut.safe_get(csv_dict, "lat", 0))
                course = int(ut.safe_get(csv_dict, "course", 0))
                speed = int(ut.safe_get(csv_dict, "speed", 0))
                status = int( ut.safe_get(csv_dict, "status", 0))

                # AFAICT the AISHUB dataset offers no particular distinction of SAR aircraft. As long as there is no implementation of message 9 (for
                # which the speed unit has to be whole knots rather than deciknots, for lack of better, the leading 3 characters of the MMSI of an SAR
                # aircraft are assumed to always be '111' (which it indeed should according to the applicable ITU standard).
                if mmsi_string[:3] == '111' : speed *= 10

                aivdm_message = ai.AISStaticAndVoyageReportMessage(mmsi = mmsi, imo = imo, callsign = callsign, shipname = shipname, shiptype = shiptype, to_bow = to_bow, to_stern = to_stern, to_port = to_port, to_starboard = to_starboard, draught = draught, destination = destination, month = month, day = day, hour = hour, minute = minute)
                aivdm_instance = ai.AIS(aivdm_message)
                aivdm_payload += aivdm_instance.build_payload(False) + ' '

                aivdm_message = ai.AISPositionReportMessage(mmsi = mmsi, lon = lon, lat = lat, sog = speed, cog = course, ts = sec, status = status)
                aivdm_instance = ai.AIS(aivdm_message)
                aivdm_payload += aivdm_instance.build_payload(False) + ' '

                #aivdm_payloads.append(aivdm_payload)

    #            rt.logging.debug("mmsis", mmsis, "speeds", speeds, "courses", courses, "statuses", statuses, "length_offsets", length_offsets, "width_offsets", width_offsets)
    #            datetime_origin = datetime.datetime.fromtimestamp(int(timestamp))
    #            origin_timestamp = datetime_origin.timetuple()
    #            sec = origin_timestamp.tm_sec
    #
    #            for mmsi, speed, course, status, length_offset, width_offset in zip(mmsis, speeds, courses, statuses, length_offsets, width_offsets) :
    #
    #                latitude_degs = float(latitude)
    #                longitude_degs = float(longitude)
    #
    #                latitude_degs += float ( length_offset / ( 1 * 1852 ) * 1/60 )
    #                latitude_min_fraction = int(latitude_degs * 60 * 10000)
    #                rt.logging.debug("latitude_degs", latitude_degs, "latitude_min_fraction", latitude_min_fraction)
    #
    #                latitude_factor = math.cos(latitude_degs/180 * math.pi)
    #                longitude_degs += float ( width_offset / ( latitude_factor * 1852 ) * 1/60 )
    #                longitude_min_fraction = int(longitude_degs * 60 * 10000)
    #                rt.logging.debug("longitude_degs", longitude_degs, "longitude_min_fraction", longitude_min_fraction)
    #
    #                speed_deci_kts = int(speed * 10)
    #                course_deci_degs = int(course * 10)
    #
    #                aivdm_message = ai.AISPositionReportMessage(mmsi = mmsi, lon = longitude_min_fraction, lat = latitude_min_fraction, sog = speed_deci_kts, cog = course_deci_degs, ts = sec, status = status)
    #                aivdm_instance = ai.AIS(aivdm_message)
    #                aivdm_payload = aivdm_instance.build_payload(False)
    #                rt.logging.debug("aivdm_payload", aivdm_payload)
    #
    #                aivdm_payloads.append(aivdm_payload)

            except(ValueError, bitstring.CreationError) as e :

                rt.logging.exception(e)

        rt.logging.debug("aivdm_payload", aivdm_payload)
        aivdm_payloads.append(aivdm_payload)

        return csv_dict, aivdm_payloads


    def aivdm_area_notice_circle_from_pos(self, mmsi, latitude, longitude) :

        #aivdm_payload = self.prepend

        try :

            latitude_degs = float(latitude)
            latitude_min_fraction = int(latitude_degs * 60 * 1000)
            rt.logging.debug("latitude_degs", latitude_degs, "latitude_min_fraction", latitude_min_fraction)

            longitude_degs = float(longitude)
            longitude_min_fraction = int(longitude_degs * 60 * 1000)
            rt.logging.debug("longitude_degs", longitude_degs, "longitude_min_fraction", longitude_min_fraction)

            rt.logging.debug("mmsi", mmsi)
            aivdm_message = ai.AISBinaryBroadcastMessageAreaNoticeCircle(mmsi = mmsi, lon_1 = longitude_min_fraction, lat_1 = latitude_min_fraction, radius_1 = 100)
            aivdm_instance = ai.AIS(aivdm_message)
            aivdm_payload += aivdm_instance.build_payload(False)
            rt.logging.debug("aivdm_payload", aivdm_payload)

        except ValueError as e :

            rt.logging.exception(e)

        finally :

            aivdm_payload += self.append

        return aivdm_payload


    def aivdm_atons_from_pos(self, mmsis, latitude, longitude, types, names, virtual_aids, length_offsets, width_offsets) :

        aivdm_payloads = []

        try :

            rt.logging.debug("mmsis", mmsis, "length_offsets", length_offsets, "width_offsets", width_offsets)

            for mmsi, aid_type, name, virtual_aid, length_offset, width_offset in zip(mmsis, types, names, virtual_aids, length_offsets, width_offsets) :

                latitude_degs = float(latitude)
                longitude_degs = float(longitude)

                latitude_degs += float ( length_offset / ( 1 * 1852 ) * 1/60 )
                latitude_min_fraction = int(latitude_degs * 60 * 10000)
                rt.logging.debug("latitude_degs", latitude_degs, "latitude_min_fraction", latitude_min_fraction)

                latitude_factor = math.cos(latitude_degs/180 * math.pi)
                longitude_degs += float ( width_offset / ( latitude_factor * 1852 ) * 1/60 )
                longitude_min_fraction = int(longitude_degs * 60 * 10000)
                rt.logging.debug("longitude_degs", longitude_degs, "longitude_min_fraction", longitude_min_fraction)

                rt.logging.debug("mmsi", mmsi, "aid_type", aid_type, "name", name, "longitude_min_fraction", longitude_min_fraction, "latitude_min_fraction", latitude_min_fraction, "virtual_aid", virtual_aid)
                name_ext = ''
                name_ext_length = len(name) - 20
                rt.logging.debug("name_ext_length", name_ext_length)
                if name_ext_length > 0 :
                    if name_ext_length > 14 : name_ext_length = 14
                    name_ext = name[ 20 : 20 + name_ext_length ]
                    rt.logging.debug("name_ext", name_ext)
                aivdm_message = ai.AISAtonReport(mmsi = mmsi, aid_type = aid_type, name = name, lon = longitude_min_fraction, lat = latitude_min_fraction, virtual_aid = virtual_aid, name_ext = name_ext)
                aivdm_instance = ai.AIS(aivdm_message)
                aivdm_payload = aivdm_instance.build_payload(False)
                rt.logging.debug("aivdm_payload", aivdm_payload)

                aivdm_payloads.append(aivdm_payload)

        except(ValueError, bitstring.CreationError) as e :

            rt.logging.exception(e)

        return aivdm_payloads


    def aivdm_atons_from_methyd_asm(self, methyd_asm_messages = None) :

        aivdm_payloads = []

        try :

            for methyd_asm_message in methyd_asm_messages :

                #aivdm_message = ai.AISAtonReport(mmsi = mmsi, aid_type = aid_type, name = name, lon = longitude_min_fraction, lat = latitude_min_fraction, virtual_aid = virtual_aid)
                #aivdm_instance = ai.AIS(aivdm_message)
                aivdm_payload = b''
                #aivdm_payload = aivdm_instance.build_payload(False)

                aivdm_payloads.append(aivdm_payload)

        except(ValueError, bitstring.CreationError) as e :

            rt.logging.exception(e)

        return aivdm_payloads


    def get_message_ids(self, message_formats) :

        rt.logging.debug("message_formats", message_formats)
        message_ids = []
        if message_formats is not None :
            for format_index in range(0, len(message_formats)) :
                message_id = ut.safe_get(message_formats[format_index], 'message')
                if message_id is not None :
                    message_types = ut.safe_get(message_id, 'type')
                    if (type(message_types) is list) :
                        for message_type in message_types :
                            message_ids.append( [ message_type, ut.safe_get(message_id, 'fid'), ut.safe_get(message_id, 'start_pos'), format_index ] )
                    else :
                        message_ids.append( [ message_types, ut.safe_get(message_id, 'fid'), ut.safe_get(message_id, 'start_pos'), format_index ] )
                else :
                    message_ids.append( [ None, None, None, format_index ] )

        return message_ids


    def append_ais_struct_function_results(self, ais_dataset, message_format) :

        rt.logging.debug("ais_dataset", ais_dataset)
        result_dataset = {}
        function_items = { structure_tag : ut.safe_get(structure_item, 'function') for structure_tag, structure_item in message_format.items() if ut.safe_get(structure_item, 'function') is not None }
        for function_member, function_struct in function_items.items() :
            rt.logging.debug("function_member", function_member, "function_struct", function_struct)
            struct_name = ut.safe_get(function_struct, 'name')
            struct_args = ut.safe_get(function_struct, 'args')
            rt.logging.debug("struct_name", struct_name, "struct_args", struct_args)
            if struct_name is not None and struct_args is not None :
                args = { arg : ut.safe_get(ais_dataset, arg) for arg in struct_args }
                rt.logging.debug("args", args)
                func = getattr(AisCallbacks, struct_name)
                return_val = ut.instance_from_dict(func, args)
                function_eval_data = {function_member:return_val}
                rt.logging.debug("function_eval_data", function_eval_data)
                ais_dataset |= function_eval_data
        rt.logging.debug("ais_dataset", ais_dataset)

        return ais_dataset


    def repair_aivdm_sentence(self, ais_message) :

        # Talker ID has been found to be occasionally missing from beginning of sentences. Add whole or part of it if necessary.
        tag_locations = locations_of_substring(ais_message, 'VDM')
        rt.logging.debug("tag_locations", tag_locations)
        if len(tag_locations) > 0 :
            sentence_id_start = tag_locations[0]
            talker_id_start = sentence_id_start - 3
            if talker_id_start < 0 :
                talker_id = '!AI'
                ais_message = talker_id[0:-talker_id_start] + ais_message
            # If checksum is missing or in an obviously incorrect format, it is recalculated and replaced/appended
            correct_checksum_form = ( ais_message[-3] == '*' and hex_characters(ais_message[-2:]) )
            if not correct_checksum_form :
                checksum_start_index = ais_message.find('*')
                if checksum_start_index == -1 :
                    ais_message += '*' + self.get_checksum(ais_message[1:])
                else :
                    ais_message = ais_message[:checksum_start_index] + '*' + self.get_checksum(ais_message[1:checksum_start_index])
            rt.logging.debug('ais_message', ais_message)

        return ais_message


    def decode_message(self, ais_message_strings, separator) :

        mmsis = []
        message_types = []
        message_format_ids = []
        ais_datasets = []
        latitudes = []
        longitudes = []

        for ais_message_string in ais_message_strings :

            ais_message_string = str(ais_message_string)
            rt.logging.debug("ais_message_string", ais_message_string)
            ais_messages = ais_message_string.split(separator)
            rt.logging.debug("ais_messages", ais_messages)

            ais_messages_temp_file = tempfile.NamedTemporaryFile(mode = 'w+', delete = False)

            valid_ais_messages = [ msg for msg in ais_messages if msg not in [None, ""] ]
            for valid_ais_message in valid_ais_messages :

                valid_ais_message = self.repair_aivdm_sentence(valid_ais_message)
                rt.logging.debug("    valid_ais_message", valid_ais_message)
                ais_messages_temp_file.write(valid_ais_message + '\n')

            ais_messages_temp_file.close()

            try :

                for ais_message in pyais.stream.FileReaderStream(ais_messages_temp_file.name) :

                    rt.logging.debug('    ais_message', ais_message)
                    ais_data = {}

                    try :
                        ais_data_object = ais_message.decode()
                        rt.logging.debug('    ais_data_object', ais_data_object)
                        ais_data = ais_data_object.asdict()
                        rt.logging.debug('    ais_data', ais_data)
                        ais_data['type'] = ais_data.pop('msg_type')
                    except (ValueError, IndexError, pyais.exceptions.InvalidNMEAMessageException, pyais.exceptions.InvalidNMEAChecksum) as e :
                        rt.logging.exception(e)
                    rt.logging.debug('    ais_data', ais_data)

                    message_id_data = self.data_dict_from_struct(ais_data, { "mmsi":{"type":"str"}, "type":{"type":"int"}, "fid":{"type":"int"} } )
                    rt.logging.debug('message_id_data', message_id_data)
                    current_mmsi = ut.safe_str(ut.safe_get(message_id_data, 'mmsi'))
                    rt.logging.debug("type(current_mmsi)", type(current_mmsi))
                    current_message_type = ut.safe_get(message_id_data, 'type')
                    rt.logging.debug("current_message_type", current_message_type)
                    current_binary_message_format = ut.safe_get(message_id_data, 'fid')

                    ais_dataset = None

                    rt.logging.debug("self.message_ids", self.message_ids)
                    message_types = [ message_id[0] for message_id in self.message_ids ]
                    rt.logging.debug("    message_types", message_types)
                    binary_message_formats = [ message_id[1] for message_id in self.message_ids ]
                    rt.logging.debug("    binary_message_formats", binary_message_formats)
                    format_indices = [ message_id[3] for message_id in self.message_ids ]
                    rt.logging.debug("    format_indices", format_indices)
                    message_type_indices = ut.safe_index(message_types, [current_message_type])
                    rt.logging.debug("    message_type_indices", message_type_indices)
                    current_format_indices = ut.safe_get(format_indices, message_type_indices)
                    rt.logging.debug("    current_format_indices", current_format_indices)

                    if current_format_indices not in [None, []] and not None in current_format_indices :

                        if current_message_type is not None and number_convertible(current_message_type) :
                            current_message_type = int(current_message_type)
                            ais_dataset = {}
                            ais_dataset |= message_id_data

                        current_format_index = None

                        if current_message_type in [6,8] :
                            current_message_type_index = None
                            for message_type_index in message_type_indices :
                                rt.logging.debug("binary_message_formats[message_type_index]", binary_message_formats[message_type_index])
                                rt.logging.debug("current_binary_message_format", current_binary_message_format)
                                if binary_message_formats[message_type_index] == current_binary_message_format :
                                    current_message_type_index = message_type_index
                                    rt.logging.debug("current_message_type_index", current_message_type_index)
                            if current_message_type_index is not None :
                                current_format_index = format_indices[current_message_type_index]
                            rt.logging.debug("current_format_index", current_format_index)
                        else :
                            current_format_index = current_format_indices[0]
                        message_format = {}
                        if current_format_index is not None :
                            message_format = ut.safe_get(self.message_formats, current_format_index)
                        rt.logging.debug("message_format", message_format)

                        #if message_format not in [None, {}] :

                        message_header_format = ut.safe_get(message_format, 'message')
                        rt.logging.debug("message_header_format", message_header_format)

                        mmsi_include_only = ut.safe_get(message_header_format, "mmsi_include_only")
                        rt.logging.debug("mmsi_include_only", mmsi_include_only)
                        mmsi_exclude_only = ut.safe_get(message_header_format, "mmsi_exclude_only")
                        include_current_mmsi = False
                        if mmsi_exclude_only in [None, []] :
                            include_current_mmsi = True
                        elif len(mmsi_exclude_only) > 0 :
                            if current_mmsi in mmsi_exclude_only :
                                include_current_mmsi = False
                        if mmsi_include_only in [None, []] :
                            pass
                        elif len(mmsi_include_only) > 0 :
                            include_current_mmsi = False
                            if current_mmsi in mmsi_include_only :
                                include_current_mmsi = True
                        rt.logging.debug("include_current_mmsi", include_current_mmsi)

                        if include_current_mmsi :

                            rt.logging.debug("type(current_message_type)", type(current_message_type))
                            rt.logging.debug("message_header_format", message_header_format, "mmsi_include_only", mmsi_include_only, "mmsi_exclude_only", mmsi_exclude_only, "current_mmsi", current_mmsi)

                            message_data = None
                            if current_message_type in [6,8] :
                                rt.logging.debug("ais_data", ais_data)
                                binary_message_data = self.data_dict_from_struct(ais_data, { "data":{"type":"struct"} } )
                                message_data = self.data_dict_from_struct( ut.safe_get(binary_message_data, 'data'), message_format )
                            else :
                                message_data = self.data_dict_from_struct( ais_data, message_format )
                            ais_dataset |= message_data
                            rt.logging.debug("ais_dataset", ais_dataset, "message_format", message_format)

                            self.append_struct_function_results(ais_dataset, message_format, AisCallbacks)
                            rt.logging.debug("ais_dataset", ais_dataset)

                            if current_mmsi is not None : # ut.safe_get(message_id_data, 'mmsi') is not None :
                                ais_datasets.append(ais_dataset)


            except (TypeError, ValueError, IndexError, pyais.exceptions.InvalidNMEAMessageException, pyais.exceptions.InvalidNMEAChecksum) as e :

                rt.logging.exception(e)

        for ais_dataset in ais_datasets :
            rt.logging.debug("ais_dataset", dict([(k,r) for k,r in ais_dataset.items() if r is not None]) )

        return ais_datasets
