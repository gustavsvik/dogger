#

import time
import datetime
import math
import numpy
import tempfile
import os
try:
    import pyais
except ImportError:
    pass
import gateway.runtime as rt
import gateway.aislib as ai


def number_convertible(value):

    return str(value).lstrip('-').replace('.','',1).replace('e-','',1).replace('e','',1).isdigit()


def locations_of_substring(string, substring):
    """Return a list of locations of a substring. ( https://stackoverflow.com/a/19720214 )"""
    substring_length = len(substring)    
    def recurse(locations_found, start):
        location = string.find(substring, start)
        if location != -1:
            return recurse(locations_found + [location], location+substring_length)
        else:
            return locations_found
    return recurse([], 0)


def channel_value_dict(channels_list, values_list) :

    channel_value_tuple_list = list(zip(channels_list, values_list))
    rt.logging.debug("channel_value_tuple_list", channel_value_tuple_list)
    min_common_length = min( len(channels_list), len(values_list) )
    rt.logging.debug("min_common_length", min_common_length)
    data_dict = dict(channel_value_tuple_list)
    rt.logging.debug("data_dict", data_dict)
    return data_dict


def get_channel_range_string(channels) :

    return ';;'.join([str(ch) for ch in channels]) + ';;'


def parse_delimited_string(data_string) :

    channel_data = [channel_string.split(',') for channel_string in data_string.split(';')]
    channel_list = channel_data[0::4][:-1]
    data_list = channel_data[1::4]
    rt.logging.debug("data_list", data_list)
    timestamp_list = [data[0::4][:-1] for data in data_list]
    values_list = [data[1::4] for data in data_list]
    byte_string_list = [data[3::4] for data in data_list]
    rt.logging.debug("byte_string_list", byte_string_list)
    return channel_list, timestamp_list, values_list, byte_string_list


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


def armor_separators(byte_string = None) :

    replaced_byte_string = byte_string.replace(b',', b'|').replace(b';', b'~')
    return replaced_byte_string


def de_armor_separators(byte_string = None) :

    replaced_byte_string = byte_string.replace('|', ',').replace('~', ';')
    return replaced_byte_string


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


def safe_get(dict_like_object, key, default_value = None) :
    value = default_value
    try :
        value = dict_like_object[key]
    except (KeyError, TypeError) as e :
        pass
    return value


def safe_index(list_like_object, value, non_exist_index = None) :
    index = non_exist_index
    if (type(value) is list) :
        common_values = list( set(value).intersection(set(list_like_object)) )
        value = common_values[0]
    if list_like_object is not None and value in list_like_object :
        index = list_like_object.index(value)
    return index


class TextNumeric :


    def __init__(self) :

        if self.prepend is None : self.prepend = ''
        if self.append is None : self.append = ''


    def decode_to_channels(self, char_data = None, channel_data = None, time_tuple = None, line_end = None) :

        if time_tuple is None :
            time_tuple = time.gmtime(time.time())
            rt.logging.debug("time_tuple", time_tuple)

        string_dict = {}

        if not ( None in [char_data, channel_data] ) :

            for selected_line in char_data :

                try :
                    selected_line = selected_line.decode()
                except (UnicodeDecodeError, AttributeError) :
                    pass

                for selected_tag, channels in channel_data.items() :
                    selected_tag = str(selected_tag)
                    rt.logging.debug("selected_tag", selected_tag)
                    rt.logging.debug("channels", channels)
                    channels_list = list(channels)
                    #TODO: Take into account that a single selected_line can contain several separated strings which potentially can be different NMEA/AIS sentences as identified by selected_tag. In current state, loss of data may occur.
                    if selected_tag in selected_line : 
                        rt.logging.debug("selected_tag", selected_tag, "channels_list", channels_list)
                        current_string_value = ''
                        try :
                            current_string_value = string_dict[selected_tag]
                        except KeyError as e :
                            pass
                        string_dict[selected_tag] = current_string_value + selected_line
                        if line_end is not None :
                            string_dict[selected_tag] += line_end
        rt.logging.debug("string_dict", string_dict)

        data_arrays = []

        for selected_tag, channels in channel_data.items() :
            rt.logging.debug("channel_data", channel_data, "selected_tag", selected_tag, "channels", channels)
            channels_list = sorted(list(channels))
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

                #data_array = []
                data_list = []

                if selected_tag == 'MMB' :
                    value = self.to_float(dict_string, 1)
                    #data_array = [ dict( [ (channels_list[0], dict_string) , (channels_list[1], [value]) ] ) ]
                    data_list = [ dict_string, [ value ] ]

                if selected_tag == 'VDM' :
                    ais_datasets = self.data_from_ais([dict_string], line_end)
                    #mmsis = [ safe_get(dataset, "mmsi") for dataset in ais_datasets ]
                    #message_format_ids = [ safe_get(dataset, "fid") for dataset in ais_datasets ]
                    #message_types = [ safe_get(dataset, "type") for dataset in ais_datasets ]
                    #latitudes = [ safe_get(dataset, "lat") for dataset in ais_datasets ]
                    #longitudes = [ safe_get(dataset, "lon") for dataset in ais_datasets ]
                    #print('mmsis', mmsis, 'message_types', message_types, 'message_format_ids', message_format_ids, 'latitudes', latitudes, 'longitudes', longitudes)
                    rt.logging.debug("ais_datasets", ais_datasets)
                    #data_array = [ dict( [ (channels_list[0], dict_string) , (channels_list[1], ais_datasets) ] ) ]
                    data_list = [ dict_string, ais_datasets ]

                if selected_tag == 'VDO' :
                    latitude, longitude = self.aivdo_to_pos(dict_string.split(line_end)[0])
                    #data_array = [ dict( [ (channels_list[0], dict_string) , (channels_list[1], [latitude]) , (channels_list[2], [longitude]) ] ) ]
                    data_list = [ dict_string, [latitude], [longitude] ]

                if selected_tag == 'ALV' :
                    #data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]
                    data_list = [ dict_string ]

                if selected_tag == 'ALR' :
                    #data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]
                    data_list = [ dict_string ]

                if selected_tag == 'TTM' :
                    #data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]
                    data_list = [ dict_string ]

                if selected_tag == 'GGA' :
                    rt.logging.debug("channels_list", channels_list, "dict_string", dict_string, "time_tuple", time_tuple)
                    validated_timestamp, timestamp_microsecs, latitude, longitude = self.gga_to_time_pos(dict_string, time_tuple)
                    rt.logging.debug("[latitude]", [latitude], "[longitude]", [longitude])
                    #data_dict = None
                    if not ( None in [latitude, longitude] ) :
                        gga_string_valid_time = self.gga_from_time_pos_float(validated_timestamp, latitude, longitude)
                        rt.logging.debug("gga_string_valid_time", gga_string_valid_time)
                        #data_dict = channel_value_dict(channels_list, [ gga_string_valid_time, [latitude], [longitude] ])
                        data_list = [ gga_string_valid_time, [latitude], [longitude] ]
                    #data_array = [data_dict]

                if selected_tag == 'GLL' :
                    #data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]
                    data_list = [ dict_string ]

                if selected_tag == 'VTG' :
                    #data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]
                    data_list = [ dict_string ]

                if selected_tag == 'RSA' :
                    #data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]
                    data_list = [ dict_string ]

                if selected_tag == 'XDR' :
                    #data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]
                    data_list = [ dict_string ]

                data_array = [ channel_value_dict( channels_list, data_list ) ]

                rt.logging.debug("data_array", data_array)
                if len(data_array) > 0 :
                    data_arrays.append( dict( [ (selected_tag, data_array) ] ) )

        rt.logging.debug("data_arrays", data_arrays)
        return data_arrays



class Nmea(TextNumeric) :


    def __init__(self, prepend = None, append = None, message_formats = None) :

        self.prepend = prepend
        self.append = append
        self.message_formats = message_formats

        TextNumeric.__init__(self)

        self.message_ids = self.get_message_ids(self.message_formats)


    def get_checksum(self, nmea_data) :

        nmea_bytearray = bytes(nmea_data, encoding='utf8')
        checksum = 0
        for i in range(0, len(nmea_bytearray)) :
            if nmea_bytearray[i] != 44 :
                checksum = checksum ^ nmea_bytearray[i]
        checksum_hex = hex(checksum)
        return checksum_hex[2:]


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
        time_string = "{:.{}f}".format(origin_timetuple.tm_hour * 10000 + origin_timetuple.tm_min * 100 + origin_timetuple.tm_sec, 2)

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

        #rt.logging.debug("nmea_string", nmea_string)
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

        if len(nmea_fields) > 5 : 
            if int(float(nmea_fields[1])) >= 0 and int(float(nmea_fields[1])) <= 235959 :
                hour, minute, second, microsec = self.time_to_time_members(nmea_fields[1])
            latitude, longitude = self.pos_to_float(nmea_fields[2], nmea_fields[3], nmea_fields[4], nmea_fields[5])

        available_datetime = datetime.datetime(year, month, monthday, hour, minute, second, microsec)
        timestamp_secs = int(available_datetime.timestamp())

        return timestamp_secs, microsec, latitude, longitude


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


    def aivdm_from_static(self, mmsi, call_sign, vessel_name, ship_type) :

        aivdm_payload = self.prepend

        try :
            rt.logging.debug(mmsi, call_sign, vessel_name, ship_type)
            aivdm_message = ai.AISStaticAndVoyageReportMessage(mmsi = mmsi, callsign = call_sign, shipname = vessel_name, shiptype = ship_type, imo = 0)
            aivdm_instance = ai.AIS(aivdm_message)
            aivdm_payload += aivdm_instance.build_payload(False)
            rt.logging.debug("aivdm_payload", aivdm_payload)
        except ValueError as e :
            aivdm_payload += ''
            rt.logging.exception(e)
        finally :
            aivdm_payload += self.append

        return aivdm_payload


    def aivdm_from_pos(self, mmsi, timestamp, latitude, longitude, status) :

        aivdm_payload = self.prepend

        try :

            latitude_degs = float(latitude)
            rt.logging.debug("latitude_degs", latitude_degs)
            latitude_min_fraction = int(latitude_degs * 60 * 10000)

            longitude_degs = float(longitude)
            rt.logging.debug("longitude_degs", longitude_degs)
            longitude_min_fraction = int(longitude_degs * 60 * 10000)

            datetime_origin = datetime.datetime.fromtimestamp(int(timestamp))
            origin_timestamp = datetime_origin.timetuple()
            sec = origin_timestamp.tm_sec
            
            aivdm_message = ai.AISPositionReportMessage(mmsi = mmsi, lon = longitude_min_fraction, lat = latitude_min_fraction, ts = sec, status = status)
            aivdm_instance = ai.AIS(aivdm_message)
            aivdm_payload += aivdm_instance.build_payload(False)
            rt.logging.debug("aivdm_payload", aivdm_payload)

        except ValueError as e :

            aivdm_payload += ''
            rt.logging.exception(e)

        finally :

            aivdm_payload += self.append

        return aivdm_payload


    def aivdm_area_notice_circle_from_pos(self, mmsi, latitude, longitude) :

        aivdm_payload = self.prepend

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

            aivdm_payload += ''
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
                aivdm_message = ai.AISAtonReport(mmsi = mmsi, aid_type = aid_type, name = name, lon = longitude_min_fraction, lat = latitude_min_fraction, virtual_aid = virtual_aid)
                aivdm_instance = ai.AIS(aivdm_message)
                aivdm_payload = aivdm_instance.build_payload(False)
                rt.logging.debug("aivdm_payload", aivdm_payload)

                aivdm_payloads.append(aivdm_payload)

        except ValueError as e :

            rt.logging.exception(e)

        finally :

            pass

        return aivdm_payloads


    def get_message_ids(self, message_formats) :

        message_ids = []
        if message_formats is not None :
            for format_index in range(0, len(message_formats)) :
                message_id = safe_get(message_formats[format_index], 'message')
                if message_id is not None :
                    message_types = safe_get(message_id, 'type')
                    if (type(message_types) is list) :
                        for message_type in message_types :
                            message_ids.append( [ message_type, safe_get(message_id, 'fid'), safe_get(message_id, 'start_pos'), format_index ] )
                    else :
                        message_ids.append( [ message_types, safe_get(message_id, 'fid'), safe_get(message_id, 'start_pos'), format_index ] )
                else :
                    message_ids.append( [ None, None, None, format_index ] )

        return message_ids


    def parse_ais_struct(self, message_string, message_structure) :

        return_dict = dict()

        bit_offset = None

        for structure_tag, structure_item in message_structure.items() :

            if structure_tag in ['message'] :
                bit_offset = safe_get(structure_item, 'start_pos')

            if structure_tag not in ['message'] :

                bits_arg = safe_get(structure_item, 'bits')
                type_arg = safe_get(structure_item, 'type')
                if type_arg is not None : type_arg = str(type_arg)

                add_arg = safe_get(structure_item, 'add')
                if add_arg is not None : add_arg = float(add_arg)
                div_arg = safe_get(structure_item, 'div')
                if div_arg is not None : div_arg = float(div_arg)
                round_arg = safe_get(structure_item, 'round')
                if round_arg is not None : round_arg = int(round_arg)

                underflow_arg = safe_get(structure_item, 'underflow')
                if underflow_arg is not None : underflow_arg = int(underflow_arg)
                overflow_arg = safe_get(structure_item, 'overflow')
                if overflow_arg is not None : overflow_arg = int(overflow_arg)
                overflow_var_flag_pos = safe_get(structure_item, 'overflow_var_flag')
                overflow_var_flag_arg = None

                nosensor_arg = safe_get(structure_item, 'nosensor')
                if nosensor_arg is not None : nosensor_arg = int(nosensor_arg)
                novalue_arg = safe_get(structure_item, 'novalue')

                return_val = None

                if bits_arg is not None and not None in bits_arg :

                    if overflow_var_flag_pos is not None : 
                        overflow_var_flag_field_string = data_field_binary_string(message_string, bit_offset, overflow_var_flag_pos[0], overflow_var_flag_pos[1])
                        overflow_var_flag_arg = bool(overflow_var_flag_field_string)
                        bits_arg[0] += overflow_var_flag_pos[1] - overflow_var_flag_pos[0] + 1
                    return_val = data_field_binary_string(message_string, bit_offset, bits_arg[0], bits_arg[1])

                    if return_val is not None and number_convertible(return_val) and type_arg is not None and type_arg not in ["int", "float", "bool"] :

                        if type_arg[0] in ['u','U','e'] :
                            return_val = int(return_val, 2)
                        elif type_arg[0] in ['i','I'] :
                            return_val = twos_comp( int(return_val, 2), len(return_val) )

                if return_val is None : 
                    return_val = safe_get(message_string, structure_tag)

                if return_val is not None and number_convertible(return_val) :
                    int_return_val = None
                    try : 
                        int_return_val = int(return_val)
                    except ValueError :
                        int_return_val = int(round(float(return_val)))
                    if underflow_arg is not None and int_return_val == underflow_arg :
                        return_val = float('-inf')
                    elif ( overflow_arg is not None and int_return_val == overflow_arg ) or ( overflow_var_flag_arg is not None and overflow_var_flag_arg == True ) :
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

                if return_val is not None and number_convertible(return_val) and type_arg is not None and ( type_arg in ["int", "float"] or type_arg[0] in ['U','I'] ) :

                    if round_arg is not None :
                        return_val = round(float(return_val), round_arg)

                    if type_arg == "int" : 
                        return_val = int(round(float(return_val)))
                    elif type_arg == "float" :
                        return_val = float(return_val)

                if return_val is not None and type_arg is not None and type_arg not in ["bool"] and type_arg[0] in ['b'] :

                    return_val = bool(return_val)

                return_dict[structure_tag] = return_val

        return return_dict


    def data_from_ais(self, ais_message_strings, separator) :

        mmsis = []
        message_types = []
        message_format_ids = []
        ais_datasets = []
        latitudes = []
        longitudes = []

        #for ais_message_string in ais_message_strings :
        #    ais_message_string = str(ais_message_string)
        #    print("ais_message_string", ais_message_string)
        #    ais_messages = ais_message_string.split(separator)
        #    f = tempfile.NamedTemporaryFile(mode='w+', delete=False)
        #    for ais_message in ais_messages :
        #        f.write(ais_message + '\n')
        #    f.close()
        #    for msg in pyais.stream.FileReaderStream(f.name):
        #        decoded_message = msg.decode()
        #        ais_content = decoded_message.content
        #        print('ais_content', ais_content)
        #    os.unlink(f.name) # delete the file after

        for ais_message_string in ais_message_strings :

            ais_message_string = str(ais_message_string)
            print("ais_message_string", ais_message_string)
            ais_messages = ais_message_string.split(separator)
            rt.logging.debug("ais_messages", ais_messages)

            ais_messages_temp_file = tempfile.NamedTemporaryFile(mode = 'w+', delete = False)
            for ais_message in ais_messages :
                ais_messages_temp_file.write(ais_message + '\n')
            ais_messages_temp_file.close()

            try :

                for ais_message in pyais.stream.FileReaderStream(ais_messages_temp_file.name) :

                    #try :
                    #    ais_message = ais_message.decode()
                    #except (UnicodeDecodeError, AttributeError) :
                    #    pass

                    ais_data = {}

                    try :
                        #try :
                        #    ais_message = ais_message.encode('UTF-8')
                        #except (UnicodeDecodeError, AttributeError) :
                        #    pass
                        #ais_data = pyais.NMEAMessage(ais_message).decode()
                        ais_data = ais_message.decode().content
                    except (ValueError, IndexError, pyais.exceptions.InvalidNMEAMessageException) as e :
                        pass #rt.logging.exception(e)
                    print('ais_data', ais_data)

                    message_id_data = self.parse_ais_struct(ais_data, { "mmsi":{"type":"str"}, "type":{"type":"int"} } )

                    ais_dataset = None

                    current_message_type = safe_get(message_id_data, 'type')
                    if current_message_type is not None and number_convertible(current_message_type) :
                        
                        current_message_type = int(current_message_type)

                        ais_dataset = {}

                        #ais_sentence = None
                        #if b'VDM' in ais_message : ais_sentence = 'VDM'
                        #elif b'VDO' in ais_message : ais_sentence = 'VDO'
                        #ais_dataset["sentence"] = ais_sentence

                        ais_dataset |= message_id_data

                    message_types = [ message_id[0] for message_id in self.message_ids ]
                    format_indices = [ message_id[3] for message_id in self.message_ids ]
                    current_message_type_index = safe_index(message_types, current_message_type)
                    current_format_index = safe_get(format_indices, current_message_type_index)
                    if current_format_index is not None :
                        message_format = self.message_formats[current_format_index]
                        message_data = None
                        if current_message_type in [6,8] :
                            binary_message_data = self.parse_ais_struct(ais_data, { "data":{"type":"struct"} } )
                            message_data = self.parse_ais_struct( safe_get(binary_message_data, 'data'), message_format )
                        else :
                            message_data = self.parse_ais_struct( ais_data, message_format )
                        ais_dataset |= message_data

                    # if message_id_data['type'] is not None and int(message_id_data['type']) in [1,2,3,9,4,5,18,19,20,24,6,8] :

                        # if int(message_id_data['type']) in [4,5] :

                            # ais_dataset |= self.parse_ais_struct(ais_data, { "minute":{"type":"int", "novalue":60}, "hour":{"type":"int", "novalue":24}, "day":{"type":"int", "novalue":0}, "month":{"type":"int", "novalue":0} } )

                        # if int(message_id_data['type']) in [6,8] :

                            # ais_dataset |= self.parse_ais_struct(ais_data, { "fid":{"type":"int"} } )
                            # for i in range(0, len(self.message_ids) ) :
                                # message_id = self.message_ids[i]
                                # if message_id[1] == safe_get(ais_dataset,"fid") :
                                    # message_format = self.message_formats[i]
                                    # message_data = self.parse_ais_struct(ais_data, { "data":{"type":"struct"} } )
                                    # ais_dataset |= self.parse_ais_struct(message_data['data'], message_format)

                        # if message_id_data['type'] is not None and int(message_id_data['type']) in [1,2,3,9,4,18,19] :

                            # ais_dataset |= self.parse_ais_struct(ais_data, { "second":{"type":"int", "novalue":[61,62,63], "nosensor":60},  "lon":{"type":"int", "div":1/600000, "novalue":181}, "lat":{"type":"int", "div":1/600000, "novalue":91}, "accuracy":{"type":"bool"} } )

                            # if int(message_id_data['type']) in [4] :

                                # ais_dataset |= self.parse_ais_struct(ais_data, { "year":{"type":"int", "novalue":0} } )

                            # if int(message_id_data['type']) in [1,2,3,9,18,19] :

                                # ais_dataset |= self.parse_ais_struct(ais_data, { "course":{"type":"float", "round": 1, "novalue":360}, "speed":{"type":"float", "round": 1, "novalue":1023, "overflow":1022} } )

                                # if int(message_id_data['type']) in [1,2,3,18,19] :

                                    # ais_dataset |= self.parse_ais_struct(ais_data, { "heading":{"type":"float", "round": 1, "novalue":511} } )

                                    # if int(message_id_data['type']) in [1,2,3] :

                                        # ais_dataset |= self.parse_ais_struct(ais_data, { "turn":{"type":"float", "round": 1, "novalue":-128, "underflow":-127, "overflow":127} } )

                        # if message_id_data['type'] is not None and int(message_id_data['type']) in [5,24] :

                            # ais_dataset |= self.parse_ais_struct(ais_data, { "callsign":{"type":"str"}, "shipname":{"type":"str"} } )

                    if message_id_data['mmsi'] is not None :

                        ais_datasets.append(ais_dataset)

            except (ValueError, pyais.exceptions.InvalidNMEAMessageException) as e :

                    rt.logging.exception(e)

        for ais_dataset in ais_datasets :
            print("ais_dataset", dict([(k,r) for k,r in ais_dataset.items() if r is not None]) )

        return ais_datasets  #, mmsis, message_types, message_format_ids, latitudes, longitudes
