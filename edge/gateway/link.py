#


import time
import datetime
import json
import socket
import struct

try : from cryptography.fernet import Fernet
except ImportError : pass

import gateway.runtime as rt
import gateway.utils as ut

import gateway.persist as ps
import gateway.transform as tr
import gateway.api as ap



class GetDBDataJson(ap.HttpMaint):


    def __init__(self, start_delay = None, transmit_rate = None, ip_list = None, http_scheme = None, maint_api_url = None, max_connect_attempts = None, table_label = None, id_range = None, config_filepath = None, config_filename = None):

        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.maint_api_url = maint_api_url
        self.max_connect_attempts = max_connect_attempts
        self.table_label = table_label
        self.id_range = id_range

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        ap.HttpMaint.__init__(self)



class DirectUpload(ap.HttpHost, ap.HttpClient):


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, ip_list = None, http_scheme = None, host_api_url = None, client_api_url = None, max_connect_attempts = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.host_api_url = host_api_url
        self.client_api_url = client_api_url
        self.max_connect_attempts = max_connect_attempts

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        ap.HttpClient.__init__(self)
        ap.HttpHost.__init__(self)



class SqlHttp(ap.HttpHost, ap.HttpClient):


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, http_scheme = None, max_age = None, host_api_url = None, client_api_url = None, max_connect_attempts = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.max_age = max_age
        self.host_api_url = host_api_url
        self.client_api_url = client_api_url
        self.max_connect_attempts = max_connect_attempts

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        ap.HttpClient.__init__(self)
        ap.HttpHost.__init__(self)

        self.sql = ps.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)


    def upload_data(self, channels, timestamps, values, byte_strings, ip_list) :
        #if self.channels is not None and ( self.channels == set() or channel in self.channels ) :
        for current_ip in ip_list :
            for channel, timestamp, value, byte_string in zip(channels, timestamps, values, byte_strings) : #range(len(values)) :
                try :
                    if not ( None in [channel, timestamp, value, byte_string] ) :
                        res = self.send_request(start_time = -9999, end_time = -9999, duration = 10, unit = 1, delete_horizon = 3600, ip = current_ip)
                        armored_byte_string = tr.armor_separators_csv(byte_string)
                        rt.logging.debug("armored_byte_string", armored_byte_string)
                        data_string = str(channel) + ';' + str(timestamp) + ',' + str(value) + ',,' + armored_byte_string.decode() + ',;'
                        rt.logging.debug("data_string", data_string)
                        res = self.set_requested(data_string, ip = current_ip)
                except Exception as e :
                    rt.logging.exception(e)


    def run(self) :

        time.sleep(self.start_delay)

        while True :

            #try:
            self.sql.connect_db()
            rt.logging.debug("self.channels", self.channels, "self.max_age", self.max_age)
            channels, times, values, byte_strings = self.sql.get_stored(from_channels = self.channels, max_age = self.max_age)
            rt.logging.debug("channels", channels, "times", times, "values", values, "byte_strings", byte_strings)
            self.upload_data(channels, times, values, byte_strings, self.ip_list) #[ip])
            #except (pymysql.err.OperationalError, pymysql.err.Error) as e :
            #    rt.logging.exception(e)
            #finally :
            self.sql.close_db_connection()

            time.sleep(1/self.transmit_rate)



class Replicate(ap.HttpHost):


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, http_scheme = None, host_api_url = None, max_connect_attempts = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.host_api_url = host_api_url
        self.max_connect_attempts = max_connect_attempts

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        ap.HttpHost.__init__(self)

        self.sql = ps.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)

        self.clear_channels()


    def clear_channels(self):

        for ip in self.ip_list :

            cleared_channels = ''
            r_clear = self.clear_data_requests(ip)
            rt.logging.debug("r_clear", r_clear)
            if r_clear is not None:
                try:
                    requested_data = r_clear.json()
                    cleared_channels = requested_data["returnstring"]
                except ValueError as e:  # includes simplejson.decoder.JSONDecodeError
                    rt.logging.debug("Decoding JSON has failed", e)
            rt.logging.debug("cleared_channels",cleared_channels)


    def run(self):

        time.sleep(self.start_delay)

        while True:

            for ip in self.ip_list :

                data_string = ''
                r_get = self.get_requested(ip)
                rt.logging.debug("r_get", r_get)

                if r_get is not None:
                    try:
                        requested_data = r_get.json()
                        data_string = requested_data['returnstring']
                    except ValueError as e:  # includes simplejson.decoder.JSONDecodeError
                        rt.logging.debug("Decoding JSON has failed", e)
                rt.logging.debug("data_string",data_string)
                channel_list, timestamp_list = tr.parse_channel_timestamp_string(data_string)
                rt.logging.debug("channel_list", channel_list, "timestamp_list", timestamp_list)
                return_string = None
                return_string = self.sql.get_requests(channel_list, timestamp_list)
                rt.logging.debug("return_string", return_string)
                if return_string is not None :
                    r_post = self.set_requested(return_string, ip)
                    rt.logging.debug("r_post", r_post)
            time.sleep(1/self.transmit_rate)



class HttpSql(ap.HttpClient):


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, http_scheme = None, client_api_url = None, max_connect_attempts = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.client_api_url = client_api_url
        self.max_connect_attempts = max_connect_attempts

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        ap.HttpClient.__init__(self)

        self.sql = ps.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)


    def run(self):

        time.sleep(self.start_delay)

        while True:

            for ip in self.ip_list :

                rt.logging.debug("ip", ip)
                data_string = ''
                r_get = self.get_uploaded(ip)
                rt.logging.debug("r_get", r_get)

                if r_get is not None:
                    try:
                        requested_data = r_get.json()
                        data_string = requested_data['returnstring']
                    except ValueError as e:  # includes simplejson.decoder.JSONDecodeError
                        rt.logging.debug("Decoding JSON has failed", e)
                rt.logging.debug("data_string", data_string)

                channel_list, times_list, values_list, byte_string_lists = tr.parse_delimited_string(data_string)
                rt.logging.debug("channel_list", channel_list, "times_list", times_list, "values_list", values_list, "byte_string_lists", byte_string_lists)
                rt.logging.debug("byte_string_lists", byte_string_lists)
                de_armored_byte_string_lists = []
                for byte_string_list in byte_string_lists :
                    de_armored_byte_string_list = []
                    for byte_string in byte_string_list :
                        de_armored_byte_string = tr.de_armor_separators_csv(byte_string)
                        de_armored_byte_string_list.append(de_armored_byte_string)
                    de_armored_byte_string_lists.append(de_armored_byte_string_list)
                rt.logging.debug("de_armored_byte_string_lists", de_armored_byte_string_lists)
                no_of_inserted_rows = self.sql.store_new_acquired_list(channel_list, times_list, values_list, de_armored_byte_string_lists)

            time.sleep(1/self.transmit_rate)



class SqlUdp(ap.UdpSend):


    def __init__(self) :

        ap.UdpSend.__init__(self)

        self.sql = ps.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)


    def run(self) :

        time.sleep(self.start_delay)

        while True :

            #try:
            self.sql.connect_db()
            channels, timestamps, values, byte_strings = self.sql.get_stored(from_channels = self.channels, max_age = self.max_age)
            rt.logging.debug("channels", channels, "timestamps", timestamps, "values", values)
            for ip in self.ip_list :
                rt.logging.debug(channels, timestamps, values, ip)
                self.set_requested(channels, timestamps, values, byte_strings, ip.split('/')[-1])
            #except (pymysql.err.OperationalError, pymysql.err.Error) as e :
            #    rt.logging.exception(e)
            #finally :
            self.sql.close_db_connection()

            time.sleep(1/self.transmit_rate)



class SqlUdpRawValue(SqlUdp) :


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, http_scheme = None, port = None, max_age = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.port = port
        self.max_age = max_age

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlUdp.__init__(self)


    def set_requested(self, channels, timestamps, values, strings, ip = '127.0.0.1') :

        rt.logging.debug("ip", ip, "self.port", self.port)
        all_data_bytes = b""
        for channel, timestamp, value in zip(channels, timestamps, values) : #range(len(values)) :
            try :
                if not (None in [channel, timestamp, value]) :
                    rt.logging.debug("int(channel)", int(channel), "int(timestamp)", int(timestamp), "float(value)", float(value))
                    data_bytes = struct.pack('>HIf', int(channel), int(timestamp), float(value))   # short unsigned int, big endian
                    rt.logging.debug("data_bytes", data_bytes)
                    all_data_bytes += data_bytes
                    #try :
                    #    self.socket.sendto(data_bytes, (ip, self.port))
                    #except Exception as e :
                    #    rt.logging.exception('Exception', e)
            except Exception as e :
                rt.logging.exception('Exception', e)

        try :
            #crypto_key = b'XUFA58vllD2n41e7NZDZkyPiUCECkxFsBjF_HaKlIrI='
            #fernet = Fernet(crypto_key)
            #rt.logging.debug("all_data_bytes", all_data_bytes)
            #encrypted_string = fernet.encrypt(all_data_bytes)
            #rt.logging.debug("len(encrypted_string)", len(encrypted_string), "encrypted_string", encrypted_string)
            #rt.logging.debug(" ")
            try :
                self.socket.sendto(all_data_bytes, (ip, self.port))
            except Exception as e :
                rt.logging.exception('Exception', e)

        except Exception as e :
            rt.logging.exception('Exception', e)



class SqlUdpRawBytes(SqlUdp) :


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, http_scheme = None, port = None, max_age = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.port = port
        self.max_age = max_age

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlUdp.__init__(self)


    def set_requested(self, channels, timestamps, values, strings, ip = '127.0.0.1') :

        rt.logging.debug("ip", ip, "self.port", self.port)
        for channel, timestamp, string in zip(channels, timestamps, strings) : #range(len(values)) :
            try :
                if not (None in [channel, timestamp, string]) :
                    rt.logging.debug("int(channel)", int(channel), "int(timestamp)", int(timestamp), "string", string)
                    crypto_key = b'XUFA58vllD2n41e7NZDZkyPiUCECkxFsBjF_HaKlIrI='
                    fernet = Fernet(crypto_key)
                    encrypted_string = fernet.encrypt(string)
                    rt.logging.debug("encrypted_string", encrypted_string)
                    rt.logging.debug(" ")
                    data_bytes = struct.pack('>HI{}s'.format(len(encrypted_string)), int(channel), int(timestamp), encrypted_string)
                    rt.logging.debug("len(data_bytes)", len(data_bytes))
                    try :
                        self.socket.sendto(data_bytes, (ip, self.port))
                    except Exception as e :
                        rt.logging.exception('Exception', e)

            except Exception as e :
                rt.logging.exception('Exception', e)



class SqlUdpNmeaValue(SqlUdp) :


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, http_scheme = None, port = None, multiplier = None, decimals = None, nmea_prepend = None, nmea_append = None, max_age = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.port = port
        self.multiplier = multiplier
        self.decimals = decimals
        self.nmea_prepend = nmea_prepend
        self.nmea_append = nmea_append
        self.max_age = max_age

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlUdp.__init__(self)

        self.nmea = tr.NmeaSentence(prepend = self.nmea_prepend, append = self.nmea_append)


    def set_requested(self, channels, times, values, strings, ip = '127.0.0.1') :

        for value in values :
            if value is not None :
                nmea_string = self.nmea.from_float(multiplier = self.multiplier, decimals = self.decimals, value = value)
                rt.logging.debug("nmea_string", nmea_string)
                self.socket.sendto(nmea_string.encode('utf-8'), (ip, self.port))



class SqlUdpBytes(SqlUdp) :


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, http_scheme = None, port = None, max_age = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.port = port
        self.max_age = max_age

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlUdp.__init__(self)


    def set_requested(self, channels, timestamps, values, strings, ip = '127.0.0.1') :

        for byte_string in strings :
            rt.logging.debug("byte_string", byte_string)
            if byte_string is not None :
                self.socket.sendto(byte_string, (ip, self.port))



class SqlUdpNmeaLines(SqlUdp) :


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, http_scheme = None, port = None, max_age = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.port = port
        self.max_age = max_age

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlUdp.__init__(self)


    def set_requested(self, channels, timestamps, values, strings, ip = '127.0.0.1') :

        for byte_string in strings :
            if byte_string is not None :
                nmea_sentence_array =  byte_string.split(b' ')
                rt.logging.debug("nmea_sentence_array", nmea_sentence_array)

                for nmea_sentence in nmea_sentence_array :
                    if nmea_sentence.find(b'\n') and nmea_sentence.find(b'\r') :
                        nmea_sentence += b'\r\n'
                        rt.logging.debug("nmea_sentence", nmea_sentence)
                        self.socket.sendto(nmea_sentence, (ip, self.port))



class SqlUdpNmeaPos(SqlUdp) :


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, port = None, nmea_prepend = None, nmea_append = None, max_age = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.port = port
        self.nmea_prepend = nmea_prepend
        self.nmea_append = nmea_append
        self.max_age = max_age

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlUdp.__init__(self)

        self.nmea = tr.NmeaSentence(prepend = self.nmea_prepend, append = self.nmea_append)


    def set_requested(self, channels, timestamps, values, strings, ip = '127.0.0.1'):

        try :
            if not ( None in [ timestamps[0], values[0], values[1] ] ) :
                nmea_string = self.nmea.gll_from_time_pos_float(timestamp = timestamps[0], latitude = values[0], longitude = values[1])
                rt.logging.debug("nmea_string", nmea_string)
                self.socket.sendto(nmea_string.encode('utf-8'), (ip, self.port))

        except Exception as e:
            rt.logging.exception(e)



class SqlUdpAivdmStatic(SqlUdp):


    def __init__(self) :

        SqlUdp.__init__(self)

        self.ais = tr.Ais()


    def get_aivdm_stat_payload(self) :

        aivdm_payload = ''

        try :
            rt.logging.debug("self.mmsi", self.mmsi, "self.call_sign", self.call_sign, "self.vessel_name", self.vessel_name, "self.ship_type", self.ship_type)
            aivdm_payload = self.ais.aivdm_from_static(mmsi = self.mmsi, call_sign = self.call_sign, vessel_name = self.vessel_name, ship_type = self.ship_type)
            rt.logging.debug("aivdm_payload", aivdm_payload)
        except ValueError as e :
            aivdm_payload += ''
            rt.logging.exception(e)
        finally :
            aivdm_payload += ''
        return aivdm_payload



class SqlUdpAivdmPosition(SqlUdpAivdmStatic):


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, port = None, max_age = None, mmsi = None, vessel_name = None, call_sign = None, ship_type = None, nav_status = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.port = port
        self.max_age = max_age

        self.mmsi = mmsi
        self.vessel_name = vessel_name
        self.call_sign = call_sign
        self.ship_type = ship_type
        self.nav_status = nav_status

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlUdpAivdmStatic.__init__(self)


    def set_requested(self, channels, times, values, strings, ip = '127.0.0.1') :

        try :
            aivdm_stat_payload = self.nmea.aivdm_from_static(mmsi = self.mmsi, vessel_name = self.vessel_name, call_sign = self.call_sign, ship_type = self.ship_type)
            rt.logging.debug('aivdm_stat_payload', aivdm_stat_payload)
            self.socket.sendto(aivdm_stat_payload.encode('utf-8'), (ip, self.port))
            rt.logging.debug("list(channels)", list(channels), "times", times, "values", values, "ip", ip)
            aivdm_pos_payload = self.nmea.aivdm_from_pos(mmsi = self.mmsi, timestamp = times[0], latitude = values[0], longitude = values[1], status = self.nav_status)
            rt.logging.debug('aivdm_pos_payload', aivdm_pos_payload)
            self.socket.sendto(aivdm_pos_payload.encode('utf-8'), (ip, self.port))
        except Exception as e:
            rt.logging.exception(e)



class SqlUdpBinaryBroadcastMessageAreaNoticeCircle(SqlUdp):


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, port = None, max_age = None, mmsi = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.port = port
        self.max_age = max_age

        self.mmsi = mmsi

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlUdp.__init__(self)

        self.ais = tr.Ais()


    def set_requested(self, channels, times, values, strings, ip = '127.0.0.1') :

        try :
            rt.logging.debug("list(channels)", list(channels), "times", times, "values", values, "ip", ip)
            aivdm_area_notice_circle_payload = self.ais.aivdm_area_notice_circle_from_pos(self.mmsi, values[0], values[1])
            rt.logging.debug('aivdm_area_notice_circle_payload', aivdm_area_notice_circle_payload)
            self.socket.sendto(aivdm_area_notice_circle_payload.encode('utf-8'), (ip, self.port))
        except Exception as e:
            rt.logging.exception(e)



class SqlUdpAtonReport(SqlUdp):


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, port = None, max_age = None, length_offset = None, width_offset = None, mmsi = None, aid_type = None, name = None, virtual_aid = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.port = port
        self.max_age = max_age

        self.length_offset = length_offset
        self.width_offset = width_offset

        self.mmsi = mmsi
        self.aid_type = aid_type
        self.name = name
        self.virtual_aid = virtual_aid

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlUdp.__init__(self)

        self.ais = tr.Ais()


    def set_requested(self, channels, times, values, strings, ip = '127.0.0.1') :

        try :

            rt.logging.debug("list(channels)", list(channels), "times", times, "values", values, "ip", ip)
            aivdm_aton_payloads = self.ais.aivdm_atons_from_pos(self.mmsi, values[0], values[1], self.aid_type, self.name, self.virtual_aid, self.length_offset, self.width_offset)
            rt.logging.debug('aivdm_aton_payloads', aivdm_aton_payloads)

            for aivdm_aton_payload in aivdm_aton_payloads :
                self.socket.sendto(aivdm_aton_payload.encode('utf-8'), (ip, self.port))

        except Exception as e :

            rt.logging.exception(e)



class SqlFile(ps.IngestFile):


    def __init__(self) :

        ps.IngestFile.__init__(self)

        self.sql = ps.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)



class SqlFileRawBytes(ps.IngestFile):


    def __init__(self) :

        ps.IngestFile.__init__(self)

        self.sql = ps.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)


    def __init__(self, channels = None, start_delay = None, sample_rate = None, transmit_rate = None, gateway_database_connection = None, nmea_prepend = None, nmea_append = None, max_age = None, max_number = None, target_channels = None, file_path = None, archive_file_path = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.sample_rate = sample_rate
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.nmea_prepend = nmea_prepend
        self.nmea_append = nmea_append
        self.max_age = max_age
        self.max_number = max_number
        self.target_channels = target_channels

        self.file_path = file_path
        self.archive_file_path = archive_file_path

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlFile.__init__(self)

        self.nmea = tr.NmeaSentence(prepend = self.nmea_prepend, append = self.nmea_append)


    def run(self):

        time.sleep(self.start_delay)

        while True:

            self.sql.connect_db()
            channels, times, values, byte_strings = self.sql.get_stored(from_channels = self.channels, max_age = self.max_age, max_number = self.max_number)
            rt.logging.debug("channels", channels, "times", times, "values", values)
            #timestamp = None
            #if times not in [None, []] and type(times) is list :
            #    timestamp = times[0]
            timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate) # timestamp = timestamp,
            rt.logging.debug("self.target_channels", self.target_channels, "byte_strings", byte_strings)
            nmea_data_array = self.nmea.decode_to_channels(char_data = byte_strings, channel_data = self.target_channels)
            rt.logging.debug("nmea_data_array", nmea_data_array)
            for nmea_data in nmea_data_array :
                if nmea_data is not None :
                    rt.logging.debug("nmea_data", nmea_data)
                    (selected_tag, data_array), = nmea_data.items()
                    self.persist(target_channels = self.target_channels, data_array = data_array, selected_tag = selected_tag, timestamp_secs = timestamp_secs, timestamp_microsecs = timestamp_microsecs)
            self.sql.close_db_connection()

            time.sleep(1/self.transmit_rate)



class SqlFileAtonReport(SqlFile):


    def __init__(self, channels = None, start_delay = None, sample_rate = None, transmit_rate = None, gateway_database_connection = None, max_age = None, target_channels = None, length_offset = None, width_offset = None, mmsi = None, aid_type = None, name = None, virtual_aid = None, file_path = None, archive_file_path = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.sample_rate = sample_rate
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.max_age = max_age
        self.target_channels = target_channels

        self.length_offset = length_offset
        self.width_offset = width_offset

        self.mmsi = mmsi
        self.aid_type = aid_type
        self.name = name
        self.virtual_aid = virtual_aid

        self.file_path = file_path
        self.archive_file_path = archive_file_path

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlFile.__init__(self)

        self.ais = tr.Ais()


    def run(self):

        time.sleep(self.start_delay)

        while True:

            #try :
            self.sql.connect_db()
            channels, times, values, byte_strings = self.sql.get_stored(from_channels = self.channels, max_age = self.max_age)
            rt.logging.debug("channels", channels, "times", times, "values", values, "byte_strings", byte_strings)
            timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate)
            rt.logging.debug("timestamp_secs", timestamp_secs, "current_timetuple", current_timetuple, "timestamp_microsecs", timestamp_microsecs, "next_sample_secs", next_sample_secs)
            aivdm_aton_payloads = None
            if values not in [None, []] and type(values) is list and len(values) >= 2 :
                aivdm_aton_payloads = self.ais.aivdm_atons_from_pos(self.mmsi, values[0], values[1], self.aid_type, self.name, self.virtual_aid, self.length_offset, self.width_offset)
            rt.logging.debug('aivdm_aton_payloads', aivdm_aton_payloads)
            nmea_data_array = self.ais.decode_to_channels(char_data = aivdm_aton_payloads, channel_data = self.target_channels, time_tuple = current_timetuple, line_end = ' ' )
            rt.logging.debug('nmea_data_array', nmea_data_array)
            for nmea_data in nmea_data_array :
                rt.logging.debug('nmea_data', nmea_data)
                if nmea_data is not None :
                    (selected_tag, data_array), = nmea_data.items()
                    rt.logging.debug("self.target_channels", self.target_channels, "data_array", data_array, "selected_tag", selected_tag)
                    self.persist(target_channels = self.target_channels, data_array = data_array, selected_tag = selected_tag, timestamp_secs = timestamp_secs, timestamp_microsecs = timestamp_microsecs)
            #except (pymysql.err.OperationalError, pymysql.err.Error) as e :
            #    rt.logging.exception(e)
            #finally :
            self.sql.close_db_connection()

            time.sleep(1/self.transmit_rate)



class SqlFileAisData(SqlFile):


    def __init__(self, channels = None, start_delay = None, sample_rate = None, transmit_rate = None, gateway_database_connection = None, max_age = None, target_channels = None, message_formats = None, file_path = None, archive_file_path = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.sample_rate = sample_rate
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.max_age = max_age
        self.target_channels = target_channels
        self.message_formats = message_formats

        self.file_path = file_path
        self.archive_file_path = archive_file_path

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlFile.__init__(self)

        self.ais = tr.Ais(message_formats = self.message_formats)


    def run(self):

        time.sleep(self.start_delay)
        rt.logging.debug(self.target_channels)

        while True:

            #try :
            self.sql.connect_db()
            channels, times, values, byte_strings = self.sql.get_stored(from_channels = self.channels, max_age = self.max_age)
            rt.logging.debug("channels", channels, "times", times, "values", values, "byte_strings", byte_strings)
            timestamp = None
            try :
                if times not in [None, []] and type(times) is list :
                    timestamp = times[0]
                timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(timestamp = timestamp, sample_rate = self.sample_rate)
                rt.logging.debug("timestamp_secs", timestamp_secs, "current_timetuple", current_timetuple, "timestamp_microsecs", timestamp_microsecs, "next_sample_secs", next_sample_secs)
                nmea_data_array = self.ais.decode_to_channels(char_data = byte_strings, channel_data = self.target_channels, time_tuple = current_timetuple, line_end = ' ' )
                rt.logging.debug("nmea_data_array", nmea_data_array)
                rt.logging.debug(" ")
            except (ValueError, IndexError, TypeError) as e :
                rt.logging.debug(e)

            for nmea_data in nmea_data_array :
                rt.logging.debug('nmea_data', nmea_data)
                if nmea_data is not None :
                    (selected_tag, data_array), = nmea_data.items()
                    rt.logging.debug("selected_tag", selected_tag, "data_array", data_array)
                    self.persist(target_channels = self.target_channels, data_array = data_array, selected_tag = selected_tag, timestamp_secs = timestamp_secs, timestamp_microsecs = timestamp_microsecs)
            #except (pymysql.err.OperationalError, pymysql.err.Error) as e :
            #    rt.logging.exception(e)
            #finally :
            self.sql.close_db_connection()

            time.sleep(1/self.transmit_rate)



class SqlFilePosAisData(SqlFile):


    def __init__(self, channels = None, start_delay = None, sample_rate = None, transmit_rate = None, gateway_database_connection = None, max_age = None, target_channels = None, length_offset = None, width_offset = None, mmsi = None, vessel_name = None, call_sign = None, ship_type = None, speed = None, course = None, nav_status = None, destination = None, file_path = None, archive_file_path = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.sample_rate = sample_rate
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.max_age = max_age
        self.target_channels = target_channels

        self.length_offset = length_offset
        self.width_offset = width_offset

        self.mmsi = mmsi
        self.vessel_name = vessel_name
        self.call_sign = call_sign
        self.ship_type = ship_type
        self.speed = speed
        self.course = course
        self.nav_status = nav_status
        self.destination = destination

        self.file_path = file_path
        self.archive_file_path = archive_file_path

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlFile.__init__(self)

        self.ais = tr.Ais()


    def run(self):

        time.sleep(self.start_delay)

        while True:

            #try :
            self.sql.connect_db()
            channels, times, values, byte_strings = self.sql.get_stored(from_channels = self.channels, max_age = self.max_age)
            rt.logging.debug("channels", channels, "times", times, "values", values, "byte_strings", byte_strings)

            timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate)
            rt.logging.debug("timestamp_secs", timestamp_secs, "current_timetuple", current_timetuple, "timestamp_microsecs", timestamp_microsecs, "next_sample_secs", next_sample_secs)

            nmea_data_array = []
            aivdm_static_payload = aivdm_pos_payload = None
            try :
                rt.logging.debug("self.mmsi", self.mmsi, "self.call_sign", self.call_sign, "self.vessel_name", self.vessel_name, "self.speed", self.speed, "self.course", self.course, "self.ship_type", self.ship_type)
                aivdm_static_payload = self.ais.aivdm_from_static(mmsis = self.mmsi, call_signs = self.call_sign, vessel_names = self.vessel_name, ship_types = self.ship_type, destinations = self.destination)
                rt.logging.debug("aivdm_static_payload", aivdm_static_payload)
                aivdm_pos_payload = self.ais.aivdm_from_pos(mmsis = self.mmsi, speeds = self.speed, courses = self.course, statuses = self.nav_status, length_offsets = self.length_offset, width_offsets = self.width_offset, timestamp = times[0], latitude = values[0], longitude = values[1])
                rt.logging.debug("aivdm_pos_payload", aivdm_pos_payload)
                nmea_data_array = self.ais.decode_to_channels(char_data = aivdm_static_payload + aivdm_pos_payload, channel_data = self.target_channels, time_tuple = current_timetuple, line_end = ' ' )
            except (ValueError, IndexError, TypeError) as e :
                rt.logging.debug(e)

            rt.logging.debug('nmea_data_array', nmea_data_array)
            for nmea_data in nmea_data_array :
                rt.logging.debug('nmea_data', nmea_data)
                if nmea_data is not None :
                    (selected_tag, data_array), = nmea_data.items()
                    rt.logging.debug("self.target_channels", self.target_channels, "data_array", data_array, "selected_tag", selected_tag)
                    self.persist(target_channels = self.target_channels, data_array = data_array, selected_tag = selected_tag, timestamp_secs = timestamp_secs, timestamp_microsecs = timestamp_microsecs)

            #except (pymysql.err.OperationalError, pymysql.err.Error) as e :
            #    rt.logging.exception(e)
            #finally :
            self.sql.close_db_connection()

            time.sleep(1/self.transmit_rate)



class SqlHttpUpdateStatic(ap.HttpHost):


    def __init__(self, channels = None, start_delay = None, sample_rate = None, transmit_rate = None, gateway_database_connection = None, max_age = None, message_formats = None, ip_list = None, http_scheme = None, host_api_url = None, max_connect_attempts = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.sample_rate = sample_rate
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.max_age = max_age

        #target_channels = {'VDM':{'mmsi|t_device/DEVICE_HARDWARE_ID':'json'}},
        #self.target_channels = target_channels
        self.target_channels = {'VDM':{0:'txt', 1:'json'}}
        self.message_formats = message_formats

        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.host_api_url = host_api_url
        self.max_connect_attempts = max_connect_attempts
        #self.file_path = file_path
        #self.archive_file_path = archive_file_path

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        #SqlFile.__init__(self)
        ap.HttpHost.__init__(self)

        self.sql = ps.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)
        self.ais = tr.Ais(message_formats = self.message_formats)


    def run(self):

        time.sleep(self.start_delay)
        rt.logging.debug(self.target_channels)

        while True:

            self.sql.connect_db()
            channels, times, values, byte_strings = self.sql.get_stored(from_channels = self.channels, max_age = self.max_age)
            rt.logging.debug("channels", channels, "times", times, "values", values, "byte_strings", byte_strings)
            timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate)
            rt.logging.debug("timestamp_secs", timestamp_secs, "current_timetuple", current_timetuple, "timestamp_microsecs", timestamp_microsecs, "next_sample_secs", next_sample_secs)

            nmea_data_array = []
            aivdm_array = []
            try :
                nmea_data_array = self.ais.decode_to_channels(char_data = byte_strings, channel_data = self.target_channels, time_tuple = current_timetuple, line_end = ' ')
                rt.logging.debug('nmea_data_array', nmea_data_array)
                #aivdm_array = []
                #try :
                aivdm_array = nmea_data_array[0]['VDM'][0][1]
                #except IndexError as e :
                #    rt.logging.exception(e)
                rt.logging.debug("aivdm_array", aivdm_array)
            except (ValueError, IndexError, TypeError) as e :
                rt.logging.debug(e)

            for aivdm_dataset in aivdm_array :

                rt.logging.debug("aivdm_dataset", aivdm_dataset)
                reduced_aivdm_dataset = dict([(k,r) for k,r in aivdm_dataset[3].items() if r is not None])
                rt.logging.debug("reduced_aivdm_dataset", reduced_aivdm_dataset)

                # imo = ut.safe_str(ut.safe_get(reduced_aivdm_dataset, "imo"))
                # ship_name = ut.safe_str(ut.safe_get(reduced_aivdm_dataset, "shipname"))
                call_sign = ut.safe_str(ut.safe_get(reduced_aivdm_dataset, "callsign"))
                mmsi = ut.safe_str(ut.safe_get(reduced_aivdm_dataset, "mmsi"))
                message_type_string = ut.safe_str(ut.safe_get(reduced_aivdm_dataset, "type"))

                # host_hardware_id = imo
                # if host_hardware_id is None :
                    # if ship_name is not None :
                        # host_hardware_id = ship_name
                # if imo is None and ship_name is None and call_sign is None :
                    # host_hardware_id = mmsi

                host_hardware_id = ut.safe_str(ut.safe_get(reduced_aivdm_dataset, "host_hardware_id"))
                host_text_id = mmsi
                rt.logging.debug("host_hardware_id", host_hardware_id, "host_text_id", host_text_id)

                #device_hardware_id = ut.safe_str(ut.safe_get(reduced_aivdm_dataset, "device_hardware_id"))
                device_text_id = 'VDM'
                device_address = ut.safe_str(ut.safe_get(reduced_aivdm_dataset, "device_address"))
                device_hardware_id = mmsi + '-' + device_text_id
                if device_address is not None : device_hardware_id += '-' + device_address
                rt.logging.debug("device_hardware_id", device_hardware_id, "device_address", device_address)

                #module_hardware_id = ut.safe_str(ut.safe_get(reduced_aivdm_dataset, "module_hardware_id"))
                module_text_id = 'VDM'
                if message_type_string is not None : module_text_id += '-' + message_type_string.zfill(2)
                format_id_string = ut.safe_str(ut.safe_get(reduced_aivdm_dataset, "fid"))
                if format_id_string is not None : module_text_id += '-' + format_id_string.zfill(2)
                area_code_string = ut.safe_str(ut.safe_get(reduced_aivdm_dataset, "dac"))
                if area_code_string is not None : module_text_id += '-' + area_code_string.zfill(3)
                module_address = ut.safe_str(ut.safe_get(reduced_aivdm_dataset, "module_address"))
                module_hardware_id = mmsi + '-' + module_text_id
                if module_address is not None : module_hardware_id += '-' + module_address
                rt.logging.debug("module_hardware_id", module_hardware_id, "module_address", module_address)

                host_address = None
                if call_sign is not None :
                    host_address = call_sign
                rt.logging.debug("host_address", host_address)

                common_description = ""

                for ip in self.ip_list :

                    existing_aivdm_dataset = {}
                    rt.logging.debug("host_hardware_id", host_hardware_id, "host_text_id", host_text_id)
                    r_post = self.get_host_data(ip, host_hardware_id = host_hardware_id, host_text_id = host_text_id)
                    rt.logging.debug("r_post", r_post)

                    if r_post is not None :

                        response_json = None
                        try :
                            response_json = r_post.json()
                        except json.decoder.JSONDecodeError as e :
                            rt.logging.exception(e)
                        rt.logging.debug("response_json", response_json)

                        existing_description = ut.safe_get(response_json, "common_description")
                        rt.logging.debug("existing_description", existing_description)
                        rt.logging.debug(" ")

                        if existing_description is not None :
                            try :
                                existing_aivdm_dataset = json.loads(existing_description)
                            except json.decoder.JSONDecodeError as e :
                                rt.logging.exception(e)

                        existing_address = ut.safe_get(response_json, "host_address")
                        if host_address is None and existing_address is not None :
                            host_address = existing_address
                        #device_address = common_address

                        existing_host_id = ut.safe_get(response_json, "host_hardware_id")
                        if host_hardware_id is None :
                            host_hardware_id = existing_host_id
                        rt.logging.debug("host_hardware_id", host_hardware_id)

                        reduced_aivdm_dataset |= {"host_hardware_id": host_hardware_id}
                        existing_aivdm_dataset |= reduced_aivdm_dataset
                        rt.logging.debug("existing_aivdm_dataset", existing_aivdm_dataset)

                        common_description += json.dumps(existing_aivdm_dataset)
                        #device_description = common_description

                    rt.logging.debug("host_hardware_id", host_hardware_id, "host_text_id", host_text_id, "host_address", host_address, "common_description", common_description)
                    if host_hardware_id is not None and host_hardware_id != "" :
                        rt.logging.debug("host_hardware_id", host_hardware_id, "host_text_id", host_text_id, "host_address", host_address, "common_description", common_description)
                        rt.logging.debug(" ")
                        r_post = self.update_static_data(ip, host_hardware_id = host_hardware_id, host_text_id = host_text_id, device_hardware_id = device_hardware_id, device_text_id = device_text_id, device_address = device_address, module_hardware_id = module_hardware_id, module_text_id = module_text_id, module_address = module_address, host_address = host_address, common_description = common_description)

            #except (pymysql.err.OperationalError, pymysql.err.Error) as e :
            #    rt.logging.exception(e)
            #finally :
            self.sql.close_db_connection()

            time.sleep(1/self.transmit_rate)



# class SqlHttpUpdateDevice(ap.HttpHost):


    # def __init__(self, channels = None, start_delay = None, sample_rate = None, transmit_rate = None, gateway_database_connection = None, max_age = None, message_formats = None, ip_list = None, http_scheme = None, host_api_url = None, max_connect_attempts = None, config_filepath = None, config_filename = None) :

        # self.channels = channels
        # self.start_delay = start_delay
        # self.sample_rate = sample_rate
        # self.transmit_rate = transmit_rate
        # self.gateway_database_connection = gateway_database_connection
        # self.max_age = max_age
        # self.target_channels = {'VDM':{0:'txt', 1:'json'}} # Create a channel (1) as a method-internal dummy target
        # self.message_formats = message_formats

        # self.ip_list = ip_list
        # self.http_scheme = http_scheme
        # self.host_api_url = host_api_url
        # self.max_connect_attempts = max_connect_attempts

        # self.config_filepath = config_filepath
        # self.config_filename = config_filename

        # ap.HttpHost.__init__(self)

        # self.sql = ps.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)
        # self.ais = tr.Ais(message_formats = self.message_formats)


    # def run(self):

        # time.sleep(self.start_delay)
        # rt.logging.debug(self.target_channels)

        # while True:

            # #try :
            # self.sql.connect_db()
            # channels, times, values, byte_strings = self.sql.get_stored(from_channels = self.channels, max_age = self.max_age)
            # rt.logging.debug("channels", channels, "times", times, "values", values, "byte_strings", byte_strings)
            # timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate)
            # rt.logging.debug("timestamp_secs", timestamp_secs, "current_timetuple", current_timetuple, "timestamp_microsecs", timestamp_microsecs, "next_sample_secs", next_sample_secs)
            # nmea_data_array = self.nmea.decode_to_channels(char_data = byte_strings, channel_data = self.target_channels, time_tuple = current_timetuple, line_end = ' ')
            # rt.logging.debug('nmea_data_array', nmea_data_array)

            # aivdm_array = []

            # try :
                # aivdm_array = nmea_data_array[0]['VDM'][0][1]
            # except IndexError as e :
                # rt.logging.exception(e)

            # for aivdm_dataset in aivdm_array :

                # reduced_aivdm_dataset = dict([(k,r) for k,r in aivdm_dataset.items() if r is not None])

                # mmsi = ut.safe_str(ut.safe_get(reduced_aivdm_dataset, "mmsi"))

                # device_hardware_id = ut.safe_str(ut.safe_get(reduced_aivdm_dataset, "device_hardware_id"))

                # device_text_id = mmsi
                # device_description = ""

                # for ip in self.ip_list :

                    # existing_aivdm_dataset = {}

                    # r_post = self.get_device_data(ip, device_hardware_id = device_hardware_id, device_text_id = device_text_id)
                    # if r_post is not None :

                        # existing_description = ut.safe_get(r_post.json(), "device_description")
                        # if existing_description is not None :
                            # try :
                                # existing_aivdm_dataset = json.loads(existing_description)
                            # except json.decoder.JSONDecodeError as e :
                                # rt.logging.exception(e)

                        # existing_hardware_id = ut.safe_get(r_post.json(), "device_hardware_id")
                        # if device_hardware_id is None :
                            # device_hardware_id = existing_hardware_id

                        # reduced_aivdm_dataset |= {"device_hardware_id": device_hardware_id}
                        # existing_aivdm_dataset |= reduced_aivdm_dataset
                        # device_description += json.dumps(existing_aivdm_dataset)

                    # r_post = self.update_device_data(ip, device_hardware_id = device_hardware_id, device_text_id = device_text_id, device_description = device_description)

            # #except (pymysql.err.OperationalError, pymysql.err.Error) as e :
            # #    rt.logging.exception(e)
            # #finally :
            # self.sql.close_db_connection()

            # time.sleep(1/self.transmit_rate)
