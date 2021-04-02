#

import time
import datetime
import math
import numpy
try:
    import pyais
except ImportError:
    pass
import gateway.runtime as rt
import gateway.aislib as ai



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

                rt.logging.debug("selected_line", selected_line)

                for selected_tag, channels in channel_data.items() :
                    rt.logging.debug("channels", channels)
                    channels_list = list(channels)

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

                data_array = []

                if selected_tag == 'MMB' :
                    value = self.to_float(dict_string, 1)
                    data_array = [ dict( [ (channels_list[0], dict_string) , (channels_list[1], [value]) ] ) ]

                if selected_tag == 'VDM' :
                    data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                if selected_tag == 'VDO' :
                    latitude, longitude = self.aivdo_to_pos(dict_string.split(line_end)[0])
                    data_array = [ dict( [ (channels_list[0], dict_string) , (channels_list[1], [latitude]) , (channels_list[2], [longitude]) ] ) ]

                if selected_tag == 'ALV' :
                    data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                if selected_tag == 'ALR' :
                    data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                if selected_tag == 'TTM' :
                    data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                if selected_tag == 'GGA' :
                    rt.logging.debug("channels_list", channels_list, "dict_string", dict_string, "time_tuple", time_tuple)
                    validated_timestamp, timestamp_microsecs, latitude, longitude = self.gga_to_time_pos(dict_string, time_tuple)
                    rt.logging.debug("[latitude]", [latitude], "[longitude]", [longitude])
                    data_dict = None
                    if not ( None in [latitude, longitude] ) :
                        gga_string_valid_time = self.gga_from_time_pos_float(validated_timestamp, latitude, longitude)
                        rt.logging.debug("gga_string_valid_time", gga_string_valid_time)
                        data_dict = channel_value_dict(channels_list, [ gga_string_valid_time, [latitude], [longitude] ])
                    rt.logging.debug("data_dict", data_dict)
                    data_array = [data_dict]

                if selected_tag == 'GLL' :
                    data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                if selected_tag == 'VTG' :
                    data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                if selected_tag == 'RSA' :
                    data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                if selected_tag == 'XDR' :
                    data_array = [ dict( [ (channels_list[0], dict_string) ] ) ]

                rt.logging.debug("data_array", data_array)
                if len(data_array) > 0 :
                    data_arrays.append( dict( [ (selected_tag, data_array) ] ) )

        rt.logging.debug("data_arrays", data_arrays)
        return data_arrays



class Nmea(TextNumeric) :


    def __init__(self, prepend = None, append = None) :

        self.prepend = prepend
        self.append = append

        TextNumeric.__init__(self)


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


    def data_from_aivdm(self, aivdm_message_strings) :

        mmsis = []
        latitudes = []
        longitudes = []

        #rt.logging.debug("aivdm_message_strings", aivdm_message_strings)

        for aivdm_message_string in aivdm_message_strings :
            aivdm_messages = aivdm_message_string.split(b' ')
            for aivdm_message in aivdm_messages :
                aivdm_data = []
                try :
                    aivdm_data = pyais.NMEAMessage(aivdm_message).decode()
                except (ValueError, IndexError, pyais.exceptions.InvalidNMEAMessageException) as e :
                    pass #rt.logging.exception(e)
                try :
                    mmsi = aivdm_data['mmsi']
                    if int(aivdm_data['type']) == 8 and int(aivdm_data['fid']) == 31 :
                        lon = int( data_field_binary_string(aivdm_data['data'], 56, 56, 80), 2 ) / 1000 / 60
                        lat = int( data_field_binary_string(aivdm_data['data'], 56, 81, 104), 2 ) / 1000 / 60
                        accuracy = bool( data_field_binary_string(aivdm_data['data'], 56, 105, 105) )
                        day = int( data_field_binary_string(aivdm_data['data'], 56, 106, 110), 2 )
                        hour = int( data_field_binary_string(aivdm_data['data'], 56, 111, 115), 2 )
                        minute = int( data_field_binary_string(aivdm_data['data'], 56, 116, 121), 2 )
                        wspeed = int( data_field_binary_string(aivdm_data['data'], 56, 122, 128), 2 )
                        wgust = int( data_field_binary_string(aivdm_data['data'], 56, 129, 135), 2 )
                        wdir = int( data_field_binary_string(aivdm_data['data'], 56, 136, 144), 2 )
                        wgustdir = int( data_field_binary_string(aivdm_data['data'], 56, 145, 153), 2 )
                        airtemp_string = data_field_binary_string(aivdm_data['data'], 56, 154, 164)
                        airtemp = twos_comp( int(airtemp_string, 2), len(airtemp_string) ) / 10
                        humidity = int( data_field_binary_string(aivdm_data['data'], 56, 165, 171), 2 )
                        dewpoint_string = data_field_binary_string(aivdm_data['data'], 56, 172, 181)
                        dewpoint = twos_comp( int(dewpoint_string, 2), len(dewpoint_string) ) / 10
                        pressure = int( data_field_binary_string(aivdm_data['data'], 56, 182, 190), 2 )
                        pressuretend = int( data_field_binary_string(aivdm_data['data'], 56, 191, 192), 2 )
                        visgreater = int( data_field_binary_string(aivdm_data['data'], 56, 193, 193), 2 )
                        visibility = int( data_field_binary_string(aivdm_data['data'], 56, 194, 200), 2 ) / 10
                        waterlevel_string = data_field_binary_string(aivdm_data['data'], 56, 201, 212)
                        waterlevel = twos_comp( int(waterlevel_string, 2), len(waterlevel_string) ) / 100
                        leveltrend = int( data_field_binary_string(aivdm_data['data'], 56, 213, 214), 2 )
                        cspeed = int( data_field_binary_string(aivdm_data['data'], 56, 215, 222), 2 ) / 10
                        cdir = int( data_field_binary_string(aivdm_data['data'], 56, 223, 231), 2 )
                        cspeed2 = int( data_field_binary_string(aivdm_data['data'], 56, 232, 239), 2 ) / 10
                        cdir2 = int( data_field_binary_string(aivdm_data['data'], 56, 240, 248), 2 )
                        cdepth2 = int( data_field_binary_string(aivdm_data['data'], 56, 249, 253), 2 )
                        cspeed3 = int( data_field_binary_string(aivdm_data['data'], 56, 254, 261), 2 ) / 10
                        cdir3 = int( data_field_binary_string(aivdm_data['data'], 56, 262, 270), 2 )
                        cdepth3 = int( data_field_binary_string(aivdm_data['data'], 56, 271, 275), 2 )
                        waveheight = int( data_field_binary_string(aivdm_data['data'], 56, 276, 283), 2 ) / 10
                        waveperiod = int( data_field_binary_string(aivdm_data['data'], 56, 284, 289), 2 )
                        wavedir = int( data_field_binary_string(aivdm_data['data'], 56, 290, 298), 2 )
                        swellheight = int( data_field_binary_string(aivdm_data['data'], 56, 299, 306), 2 ) / 10
                        swellperiod = int( data_field_binary_string(aivdm_data['data'], 56, 307, 312), 2 )
                        swelldir = int( data_field_binary_string(aivdm_data['data'], 56, 313, 321), 2 )
                        seastate = int( data_field_binary_string(aivdm_data['data'], 56, 322, 325), 2 )
                        watertemp_string = data_field_binary_string(aivdm_data['data'], 56, 326, 335)
                        watertemp = twos_comp( int(watertemp_string, 2), len(watertemp_string) ) / 10
                        preciptype = int( data_field_binary_string(aivdm_data['data'], 56, 336, 338), 2 )
                        salinity = int( data_field_binary_string(aivdm_data['data'], 56, 339, 347), 2 ) / 10
                        ice = int( data_field_binary_string(aivdm_data['data'], 56, 348, 349), 2 )
                        rt.logging.debug("mmsi", mmsi, "lon", lon, "lat", lat, "accuracy", accuracy, "day", day, "hour", hour, "minute", minute, "wspeed", wspeed, "wgust", wgust, "wdir", wdir, "wgustdir", wgustdir, "airtemp", airtemp, "humidity", humidity, "dewpoint", dewpoint, "pressure", pressure, "pressuretend", pressuretend, "visgreater", visgreater, "visibility", visibility, "waterlevel", waterlevel, "leveltrend", leveltrend, "cspeed", cspeed, "cdir", cdir, "cspeed2", cspeed2, "cdir2", cdir2, "cdepth2", cdepth2, "cspeed3", cspeed3, "cdir3", cdir3, "cdepth3", cdepth3, "waveheight", waveheight, "waveperiod", waveperiod, "wavedir", wavedir, "swellheight", swellheight, "swellperiod", swellperiod, "swelldir", swelldir, "seastate", seastate, "watertemp", watertemp, "preciptype", preciptype, "salinity", salinity, "ice", ice)

                    #mmsis.append(mmsi)
                except (TypeError, KeyError) as e :
                    pass #rt.logging.exception(e)

        return mmsis, latitudes, longitudes
