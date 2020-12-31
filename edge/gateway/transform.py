#

import time
import datetime
import math
import numpy

import gateway.runtime as rt
import gateway.aislib as ai



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
    #year = current_timestamp.tm_year
    #month = current_timestamp.tm_mon
    #monthday = current_timestamp.tm_mday

    return current_secs, current_timetuple, current_microsec_part, next_sample_secs



class Nmea :


    def __init__(self, prepend = None, append = None) :

        self.prepend = prepend
        self.append = append


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
        print("nmea_fields", nmea_fields)

        try :
            value = float(nmea_fields[index])
        except ValueError as e :
            value = -9999.0

        print("value", value)
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
            print("nmea_string", nmea_string)

        except Exception as e :

            print(e)

        finally :

            return nmea_string


    def from_time_pos(self, timestamp, latitude, longitude) :

        nmea_data = self.prepend

        try :

            latitude_dir = 'N'
            latitude_abs = abs(float(latitude))
            latitude_sign = float(latitude) / latitude_abs
            if latitude_sign < 0 : latitude_dir = 'S'
            latitude_deg = latitude_abs // 1
            latitude_min = ( latitude_abs - latitude_deg ) * 60

            longitude_dir = 'E'
            longitude_abs = abs(float(longitude))
            longitude_sign = float(longitude) / longitude_abs
            if longitude_sign < 0 : longitude_dir = 'W'
            longitude_deg = longitude_abs // 1
            longitude_min = ( longitude_abs - longitude_deg ) * 60

            datetime_origin = datetime.datetime.fromtimestamp(int(timestamp))
            origin_timestamp = datetime_origin.timetuple()
            hour = origin_timestamp.tm_hour
            min = origin_timestamp.tm_min
            sec = origin_timestamp.tm_sec
            lead_zero = ''
            if longitude_deg < 100 :
                lead_zero = '0'
                if longitude_deg < 10 :
                    lead_zero = '00'
            timestamp_string = "{:.{}f}".format(hour * 10000 + min * 100 + sec, 2)
            nmea_data += "{:.{}f}".format(latitude_deg * 100 + latitude_min, 7) + ',' + latitude_dir + ',' + lead_zero + "{:.{}f}".format(longitude_deg * 100 + longitude_min, 7) + ',' + longitude_dir + ',' + timestamp_string

        except ValueError as e :
            nmea_data += '9999.0,N,9999.0,E'
            rt.logging.exception(e)
        finally :
            nmea_data += self.append

        nmea_string = '$' + nmea_data + '*' + self.get_checksum(nmea_data) + '\n'
        print('nmea_string', nmea_string)

        return nmea_string


    def gga_to_time_pos(self, nmea_string, year, month, monthday) :

        #print("nmea_string", nmea_string)
        nmea_fields = nmea_string.split(',')
        print("nmea_fields", nmea_fields)
        orig_hour = int(float(nmea_fields[1])) // 10000
        orig_minute = ( int(float(nmea_fields[1])) - orig_hour * 10000 ) // 100
        orig_second = int(float(nmea_fields[1])) - orig_hour * 10000 - orig_minute * 100
        orig_microsec = int ( ( float(nmea_fields[1]) - orig_hour * 10000 - orig_minute * 100 - orig_second ) * 1000000 )
        datetime_origin = datetime.datetime(year, month, monthday, orig_hour, orig_minute, orig_second, orig_microsec)
        orig_secs = int(datetime_origin.timestamp())

        latitude_deg = float(nmea_fields[2]) // 100
        latitude_min = float(nmea_fields[2]) - latitude_deg * 100
        latitude = latitude_deg + latitude_min / 60
        if nmea_fields[3] == 'S' : latitude = -latitude

        longitude_deg = float(nmea_fields[4]) // 100
        longitude_min = float(nmea_fields[4]) - longitude_deg * 100
        longitude = longitude_deg + longitude_min / 60
        if nmea_fields[5] == 'W' : longitude = -longitude

        return orig_secs, orig_microsec, latitude, longitude


    def aivdm_from_static(self, mmsi, call_sign, vessel_name, ship_type) :

        aivdm_payload = self.prepend

        try :

            aivdm_message = ai.AISStaticAndVoyageReportMessage(mmsi = mmsi, callsign = call_sign, shipname = vessel_name, shiptype = ship_type, imo = 0)
            aivdm_instance = ai.AIS(aivdm_message)
            aivdm_payload += aivdm_instance.build_payload(False)
            rt.logging.debug("aivdm_payload", aivdm_payload)

        except ValueError as e :

            aivdm_payload += ''
            rt.logging.exception(e)

        finally :

            aivdm_payload += self.append


    def aivdm_from_pos(self, mmsi, timestamp, latitude, longitude) :

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
            
            aivdm_message = ai.AISPositionReportMessage(mmsi = mmsi, lon = longitude_min_fraction, lat = latitude_min_fraction, ts = sec, status = 15)
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

                print("mmsi", mmsi, "aid_type", aid_type, "name", name, "longitude_min_fraction", longitude_min_fraction, "latitude_min_fraction", latitude_min_fraction, "virtual_aid", virtual_aid)
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
