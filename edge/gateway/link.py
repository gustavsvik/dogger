#

import requests
import time
import datetime
import json
import pymysql
import socket
import struct
import http

import gateway.runtime as rt
import gateway.metadata as md
import gateway.task as ta
import gateway.persist as ps
import gateway.transform as tr



class Link(ta.LinkTask):


    def __init__(self):

        self.env = self.get_env()
        if self.ip_list is None: self.ip_list = self.env['IP_LIST']

        ta.LinkTask.__init__(self)



class Http(Link):


    def __init__(self) :

        self.env = self.get_env()
        if self.max_connect_attempts is None: self.max_connect_attempts = self.env['MAX_CONNECT_ATTEMPTS']

        Link.__init__(self)

        self.connect_attempts = 0



class HttpMaint(Http):


    def __init__(self) :

        self.env = self.get_env()
        if self.maint_api_url is None: self.maint_api_url = self.env['MAINT_API_URL']

        Http.__init__(self)


    def partition_database(self, ip = '127.0.0.1'):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            rt.logging.debug("http://" + ip + self.maint_api_url + "partition_database.php")
            raw_data = requests.post("http://" + ip + self.maint_api_url + "partition_database.php", timeout = 5, data = {"new_partition_name_date": self.new_partition_name_date, "new_partition_timestamp": self.new_partition_timestamp, "oldest_kept_partition_name_date": self.oldest_kept_partition_name_date})
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.partition_database(ip)
            else:
                exit(-1)


    def get_db_rows(self, ip = '127.0.0.1'):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            raw_data = requests.post("http://" + ip + self.maint_api_url + "get_db_rows.php", timeout = 5, data = {"table_label": self.table_label, "id_range": self.id_range})
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.get_db_rows(ip)
            else:
                exit(-1)



class HttpHost(Http):


    def __init__(self) :

        self.env = self.get_env()
        if self.host_api_url is None: self.host_api_url = self.env['HOST_API_URL']

        Http.__init__(self)


    def get_requested(self, ip = '127.0.0.1'):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            raw_data = requests.post("http://" + ip + self.host_api_url + "get_requested.php", timeout = 5, data = {"channelrange": tr.get_channel_range_string(self.channels), "duration": 10, "unit": 1})
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.get_requested(ip)
            else:
                exit(-1)


    def set_requested(self, data_string, ip = '127.0.0.1'):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            rt.logging.debug("data_string", data_string)
            raw_data = requests.post("http://" + ip + self.host_api_url + "set_requested.php", timeout = 5, data = {"returnstring": data_string})
            print("raw_data", raw_data)
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.set_requested(data_string, ip)
            else:
                exit(-1)


    def clear_data_requests(self, ip = '127.0.0.1'):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            raw_data = requests.post("http://" + ip + self.host_api_url + "clear_data_requests.php", timeout = 5, data = {"channelrange": tr.get_channel_range_string(self.channels)})
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.clear_data_requests(ip)
            else:
                exit(-1)



class HttpClient(Http):


    def __init__(self) :

        self.env = self.get_env()
        if self.client_api_url is None: self.client_api_url = self.env['CLIENT_API_URL']

        Http.__init__(self)


    def send_request(self, ip = '127.0.0.1', start_time = -9999, end_time = -9999, duration = 10, unit = 1, delete_horizon = 3600):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            d = {"channels": tr.get_channel_range_string(self.channels), "start_time": start_time, "end_time": end_time, "duration": duration, "unit": unit, "delete_horizon": delete_horizon}
            raw_data = requests.post("http://" + ip + self.client_api_url + "send_request.php", timeout = 5, data = d)
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.send_request(ip, start_time, end_time, duration, unit, delete_horizon)
            else:
                exit(-1)


    def get_uploaded(self, ip = '127.0.0.1', start_time = -9999, end_time = -9999, duration = 10, unit = 1, lowest_status = 0):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            d = {"channels": tr.get_channel_range_string(self.channels), "start_time": start_time, "end_time": end_time, "duration": duration, "unit": unit, "lowest_status": lowest_status}
            raw_data = requests.post("http://" + ip + self.client_api_url + "get_uploaded.php", timeout = 5, data = d)
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.get_uploaded(ip, start_time, end_time, duration, unit, lowest_status)
            else:
                exit(-1)



class GetDBDataJson(HttpMaint):


    def __init__(self, start_delay = None, transmit_rate = None, ip_list = None, maint_api_url = None, max_connect_attempts = None, table_label = None, id_range = None, config_filepath = None, config_filename = None):

        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.ip_list = ip_list
        self.maint_api_url = maint_api_url
        self.max_connect_attempts = max_connect_attempts
        self.table_label = table_label
        self.id_range = id_range

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        HttpMaint.__init__(self)



class DirectUpload(HttpHost, HttpClient):


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, ip_list = None, host_api_url = None, client_api_url = None, max_connect_attempts = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.ip_list = ip_list
        self.host_api_url = host_api_url
        self.client_api_url = client_api_url
        self.max_connect_attempts = max_connect_attempts

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        HttpClient.__init__(self)
        HttpHost.__init__(self)



class SqlHttp(HttpHost, HttpClient):


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, max_age = None, host_api_url = None, client_api_url = None, max_connect_attempts = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.max_age = max_age
        self.host_api_url = host_api_url
        self.client_api_url = client_api_url
        self.max_connect_attempts = max_connect_attempts

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        HttpClient.__init__(self)
        HttpHost.__init__(self)

        self.sql = ps.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)


    def upload_data(self, channels, timestamps, values, byte_strings, ip_list) :
        #if self.channels is not None and ( self.channels == set() or channel in self.channels ) :
        for current_ip in ip_list :
            for channel, timestamp, value, byte_string in zip(channels, timestamps, values, byte_strings) : #range(len(values)) :
                try :
                    if not ( None in [channel, timestamp, value, byte_string] ) :
                        res = self.send_request(start_time = -9999, end_time = -9999, duration = 10, unit = 1, delete_horizon = 3600, ip = current_ip)
                        armored_byte_string = tr.armor_separators(byte_string)
                        print("armored_byte_string", armored_byte_string)
                        data_string = str(channel) + ';' + str(timestamp) + ',' + str(value) + ',,' + armored_byte_string.decode() + ',;'
                        print("data_string", data_string)
                        res = self.set_requested(data_string, ip = current_ip)
                except Exception as e :
                    print(e)


    def run(self) :

        time.sleep(self.start_delay)

        while True :

            try:
                self.sql.connect_db()
                print("self.channels", self.channels, "self.max_age", self.max_age)
                channels, times, values, byte_strings = self.sql.get_stored(self.channels, self.max_age)
                print("channels", channels, "times", times, "values", values, "byte_strings", byte_strings)
                self.upload_data(channels, times, values, byte_strings, self.ip_list) #[ip])
            except (pymysql.err.OperationalError, pymysql.err.Error) as e :
                rt.logging.exception(e)
            finally :
                self.sql.close_db_connection()

            time.sleep(1/self.transmit_rate)



class Replicate(HttpHost):


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, host_api_url = None, max_connect_attempts = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.host_api_url = host_api_url
        self.max_connect_attempts = max_connect_attempts

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        HttpHost.__init__(self)

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
                print("data_string",data_string)
                channel_list, timestamp_list = tr.parse_channel_timestamp_string(data_string)
                print("channel_list", channel_list, "timestamp_list", timestamp_list)
                return_string = None
                return_string = self.sql.get_requests(channel_list, timestamp_list)
                print("return_string", return_string)
                if return_string is not None :
                    r_post = self.set_requested(return_string, ip)
                    print("r_post", r_post)
            time.sleep(1/self.transmit_rate)



class HttpSql(HttpClient):


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, client_api_url = None, max_connect_attempts = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.client_api_url = client_api_url
        self.max_connect_attempts = max_connect_attempts

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        HttpClient.__init__(self)

        self.sql = ps.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)


    def run(self):

        time.sleep(self.start_delay)

        while True:

            for ip in self.ip_list :

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

                channel_list, times_list, values_list, byte_string_list = tr.parse_delimited_string(data_string)
                print("channel_list", channel_list, "times_list", times_list, "values_list", values_list, "byte_string_list", byte_string_list)
                try:
                    self.sql.connect_db()
                    for channel, times, values, byte_strings in zip(channel_list, times_list, values_list, byte_string_list) :
                        channel= channel[0]
                        for timestamp, value, byte_string in zip(times, values, byte_strings) :
                            replaced_byte_string = tr.de_armor_separators(byte_string)
                            rt.logging.debug("replaced_byte_string", replaced_byte_string)
                            with self.sql.conn.cursor() as cursor :
                                # TODO: precede by SELECT to avoid INSERT attempts causing primary key violations
                                sql = "INSERT INTO t_acquired_data (ACQUIRED_TIME,CHANNEL_INDEX,ACQUIRED_VALUE,ACQUIRED_BASE64,STATUS) VALUES (" + str(timestamp) + "," + str(channel) + "," + str(value) + ",'" + replaced_byte_string + "',0)"
                                rt.logging.debug("sql", sql)
                                try:
                                    cursor.execute(sql)
                                except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                                    rt.logging.exception(e)
                                result = cursor.rowcount
                                rt.logging.debug("result", result)

                except (pymysql.err.OperationalError, pymysql.err.Error) as e:

                    rt.logging.exception(e)

                finally:

                    try:
                        self.sql.close_db_connection()
                    except pymysql.err.Error as e:
                        rt.logging.exception(e)

            time.sleep(1/self.transmit_rate)



class Udp(Link):


    def __init__(self) :

        Link.__init__(self)

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)



class UdpReceive(Udp):


    def __init__(self):

        Udp.__init__(self)

        server_address = ('', self.port)
        self.socket.bind(server_address)



class UdpSend(Udp):


    def __init__(self):

        Udp.__init__(self)



class SqlUdp(UdpSend):


    def __init__(self) :

        UdpSend.__init__(self)

        self.sql = ps.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)


    def run(self) :

        time.sleep(self.start_delay)

        while True :

            try:
                self.sql.connect_db()
                channels, times, values, byte_strings = self.sql.get_stored(self.channels, self.max_age)
                for ip in self.ip_list :
                    rt.logging.debug(channels, times, values, ip)
                    self.set_requested(channels, times, values, byte_strings, ip)
            except (pymysql.err.OperationalError, pymysql.err.Error) as e :
                rt.logging.exception(e)
            finally :
                self.sql.close_db_connection()

            time.sleep(1/self.transmit_rate)



class SqlUdpRawValue(SqlUdp) :


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, port = None, max_age = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.port = port
        self.max_age = max_age

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlUdp.__init__(self)


    def set_requested(self, channels, timestamps, values, strings, ip = '127.0.0.1') :

        print("ip", ip, "self.port", self.port)
        for channel, timestamp, value in zip(channels, timestamps, values) : #range(len(values)) :
            try :
                if not (None in [channel, timestamp, value]) :
                    print("int(channel)", int(channel), "int(timestamp)", int(timestamp), "float(value)", float(value))
                    data_bytes = struct.pack('>HIf', int(channel), int(timestamp), float(value))   # short unsigned int, big endian
                    print("len(data_bytes)", len(data_bytes))
                    try :
                        self.socket.sendto(data_bytes, (ip, self.port))
                    except Exception as e :
                        rt.logging.exception('Exception', e)
            except Exception as e :
                rt.logging.exception('Exception', e)



class SqlUdpRawBytes(SqlUdp) :


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, port = None, max_age = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.port = port
        self.max_age = max_age

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlUdp.__init__(self)


    def set_requested(self, channels, timestamps, values, strings, ip = '127.0.0.1') :

        print("ip", ip, "self.port", self.port)
        for channel, timestamp, string in zip(channels, timestamps, strings) : #range(len(values)) :
            try :
                if not (None in [channel, timestamp, string]) :
                    print("int(channel)", int(channel), "int(timestamp)", int(timestamp), "string", string)
                    data_bytes = struct.pack('>HI{}s'.format(len(string)), int(channel), int(timestamp), string)
                    print("len(data_bytes)", len(data_bytes))
                    try :
                        self.socket.sendto(data_bytes, (ip, self.port))
                    except Exception as e :
                        rt.logging.exception('Exception', e)

            except Exception as e :
                rt.logging.exception('Exception', e)



class SqlUdpNmeaValue(SqlUdp) :


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, port = None, multiplier = None, decimals = None, nmea_prepend = None, nmea_append = None, max_age = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.port = port
        self.multiplier = multiplier
        self.decimals = decimals
        self.nmea_prepend = nmea_prepend
        self.nmea_append = nmea_append
        self.max_age = max_age

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlUdp.__init__(self)

        self.nmea = tr.Nmea(prepend = self.nmea_prepend, append = self.nmea_append)


    def set_requested(self, channels, times, values, strings, ip = '127.0.0.1') :

        for value in values :
            if value is not None :
                nmea_string = self.nmea.from_float(multiplier = self.multiplier, decimals = self.decimals, value = value)
                print("nmea_string", nmea_string)
                self.socket.sendto(nmea_string.encode('utf-8'), (ip, self.port))



class SqlUdpBytes(SqlUdp) :


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, port = None, max_age = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.port = port
        self.max_age = max_age

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlUdp.__init__(self)


    def set_requested(self, channels, timestamps, values, strings, ip = '127.0.0.1') :

        for byte_string in strings :
            print("byte_string", byte_string)
            if byte_string is not None :
                self.socket.sendto(byte_string, (ip, self.port))



class SqlUdpNmeaLines(SqlUdp) :


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, port = None, max_age = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.port = port
        self.max_age = max_age

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlUdp.__init__(self)


    def set_requested(self, channels, timestamps, values, strings, ip = '127.0.0.1') :

        for byte_string in strings :
            if byte_string is not None :
                nmea_sentence_array =  byte_string.split(b' ')
                print("nmea_sentence_array", nmea_sentence_array)

                for nmea_sentence in nmea_sentence_array :
                    if nmea_sentence.find(b'\n') and nmea_sentence.find(b'\r') :
                        nmea_sentence += b'\r\n'
                        print("nmea_sentence", nmea_sentence)
                        self.socket.sendto(nmea_sentence, (ip, self.port))



class SqlUdpNmeaPos(SqlUdp) :


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, port = None, max_age = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.port = port
        self.max_age = max_age

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SqlUdp.__init__(self)

        self.nmea = tr.Nmea()


    def set_requested(self, channels, timestamps, values, strings, ip = '127.0.0.1'):

        try :
            if not ( None in [ timestamps[0], values[0], values[1] ] ) :
                nmea_string = self.nmea.gll_from_time_pos_float(timestamp = timestamps[0], latitude = values[0], longitude = values[1])
                self.socket.sendto(nmea_string.encode('utf-8'), (ip, self.port))

        except Exception as e:
            rt.logging.exception(e)



class SqlUdpAivdmStatic(SqlUdp):


    def __init__(self) :

        SqlUdp.__init__(self)

        self.nmea = tr.Nmea(prepend = '', append = '')


    def get_aivdm_stat_payload(self) :

        aivdm_payload = ''

        try :
            rt.logging.debug("self.mmsi", self.mmsi, "self.call_sign", self.call_sign, "self.vessel_name", self.vessel_name, "self.ship_type", self.ship_type)
            aivdm_payload = self.nmea.from_static_to_aivdm(mmsi = self.mmsi, call_sign = self.call_sign, vessel_name = self.vessel_name, ship_type = self.ship_type)
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
            print('aivdm_stat_payload', aivdm_stat_payload)
            self.socket.sendto(aivdm_stat_payload.encode('utf-8'), (ip, self.port))
            print("list(channels)", list(channels), "times", times, "values", values, "ip", ip)
            aivdm_pos_payload = self.nmea.aivdm_from_pos(mmsi = self.mmsi, timestamp = times[0], latitude = values[0], longitude = values[1], status = self.nav_status)
            print('aivdm_pos_payload', aivdm_pos_payload)
            self.socket.sendto(aivdm_pos_payload.encode('utf-8'), (ip, self.port))
        except Exception as e:
            print(e)



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

        self.nmea = tr.Nmea(prepend = '', append = '')


    def set_requested(self, channels, times, values, strings, ip = '127.0.0.1') :

        try :
            rt.logging.debug("list(channels)", list(channels), "times", times, "values", values, "ip", ip)
            aivdm_area_notice_circle_payload = self.nmea.aivdm_area_notice_circle_from_pos(self.mmsi, values[0], values[1])
            rt.logging.debug('aivdm_area_notice_circle_payload', aivdm_area_notice_circle_payload)
            self.socket.sendto(aivdm_area_notice_circle_payload.encode('utf-8'), (ip, self.port))
        except Exception as e:
            rt.logging.exception(e)



class SqlUdpAtonReport(SqlUdp):


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, gateway_database_connection = None, ip_list = None, port = None, max_age = None, length_offset = None, width_offset = None, mmsi = None, aid_type = None, name = None, virtual_aid = None, config_filepath = None, config_filename = None) :
        #repeat = 0, mmsi = 0, aid_type = 0, name = 0, accuracy = 0, lon = 181000, lat = 91000, to_bow = 0, to_stern = 0, to_port = 0, to_starboard = 0, epfd = 0, ts = 60, off_position = 0, raim = 0, virtual_aid = 0, assigned = 0)
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

        self.nmea = tr.Nmea(prepend = '', append = '')


    def set_requested(self, channels, times, values, strings, ip = '127.0.0.1') :

        try :

            print("list(channels)", list(channels), "times", times, "values", values, "ip", ip)
            aivdm_aton_payloads = self.nmea.aivdm_atons_from_pos(self.mmsi, values[0], values[1], self.aid_type, self.name, self.virtual_aid, self.length_offset, self.width_offset)
            print('aivdm_aton_payloads', aivdm_aton_payloads)

            for aivdm_aton_payload in aivdm_aton_payloads :
                self.socket.sendto(aivdm_aton_payload.encode('utf-8'), (ip, self.port))

        except Exception as e :

            print(e)



class SqlFile(ps.IngestFile):


    def __init__(self) :

        ps.IngestFile.__init__(self)

        self.sql = ps.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)



class SqlFileAtonReport(SqlFile):


    def __init__(self, channels = None, start_delay = None, sample_rate = None, transmit_rate = None, gateway_database_connection = None, max_age = None, target_channels = None, length_offset = None, width_offset = None, mmsi = None, aid_type = None, name = None, virtual_aid = None, file_path = None, archive_file_path = None, config_filepath = None, config_filename = None) :
        #repeat = 0, mmsi = 0, aid_type = 0, name = 0, accuracy = 0, lon = 181000, lat = 91000, to_bow = 0, to_stern = 0, to_port = 0, to_starboard = 0, epfd = 0, ts = 60, off_position = 0, raim = 0, virtual_aid = 0, assigned = 0)

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

        self.nmea = tr.Nmea()


    def run(self):

        time.sleep(self.start_delay)

        while True:

            self.sql.connect_db()
            channels, times, values, byte_strings = self.sql.get_stored(self.channels, self.max_age)
            print("channels", channels, "times", times, "values", values, "byte_strings", byte_strings) 
            timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate)
            print("timestamp_secs", timestamp_secs, "current_timetuple", current_timetuple, "timestamp_microsecs", timestamp_microsecs, "next_sample_secs", next_sample_secs) 
            aivdm_aton_payloads = None
            if len(values) > 0 :
                aivdm_aton_payloads = self.nmea.aivdm_atons_from_pos(self.mmsi, values[0], values[1], self.aid_type, self.name, self.virtual_aid, self.length_offset, self.width_offset)
            print('aivdm_aton_payloads', aivdm_aton_payloads)
            nmea_data_array = self.nmea.decode_to_channels(char_data = aivdm_aton_payloads, channel_data = self.target_channels, time_tuple = current_timetuple, line_end = ' ' )
            print('nmea_data_array', nmea_data_array)
            for nmea_data in nmea_data_array :
                print('nmea_data', nmea_data)
                if nmea_data is not None :
                    (selected_tag, data_array), = nmea_data.items()
                    print("selected_tag", selected_tag, "data_array", data_array)
                    self.write(target_channels = self.target_channels, data_array = data_array, selected_tag = selected_tag, timestamp_secs = timestamp_secs, timestamp_microsecs = timestamp_microsecs) 

            time.sleep(1/self.transmit_rate)
