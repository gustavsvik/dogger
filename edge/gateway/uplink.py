#

import requests
import time
import datetime
import json
import pymysql
import socket
import struct
import http
import math

import gateway.runtime as rt
import gateway.metadata as md
import gateway.task as t
import gateway.database as db
import gateway.utils as u
import gateway.aislib as ai




class Uplink(t.Task):


    def __init__(self):

        self.env = self.get_env()
        if self.ip_list is None: self.ip_list = self.env['IP_LIST']

        t.Task.__init__(self)



class Http(Uplink):


    def __init__(self) :

        self.env = self.get_env()
        if self.max_connect_attempts is None: self.max_connect_attempts = self.env['MAX_CONNECT_ATTEMPTS']

        Uplink.__init__(self)

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
            raw_data = requests.post("http://" + ip + self.host_api_url + "get_requested.php", timeout = 5, data = {"channelrange": u.get_channel_range_string(self.channels), "duration": 10, "unit": 1})
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
            raw_data = requests.post("http://" + ip + self.host_api_url + "set_requested.php", timeout = 5, data = {"returnstring": data_string})
            rt.logging.debug("data_string", data_string)
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
            raw_data = requests.post("http://" + ip + self.host_api_url + "clear_data_requests.php", timeout = 5, data = {"channelrange": u.get_channel_range_string(self.channels)})
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
            d = {"channels": u.get_channel_range_string(self.channels), "start_time": start_time, "end_time": end_time, "duration": duration, "unit": unit, "delete_horizon": delete_horizon}
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
            d = {"channels": u.get_channel_range_string(self.channels), "start_time": start_time, "end_time": end_time, "duration": duration, "unit": unit, "lowest_status": lowest_status}
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



class CloudDBPartition(HttpMaint):


    def __init__(self, start_delay = None, ip_list = None, maint_api_url = None, max_connect_attempts = None, new_partition_name_date = None, new_partition_timestamp = None, oldest_kept_partition_name_date = None, config_filepath = None, config_filename = None):

        self.start_delay = start_delay
        self.ip_list = ip_list
        self.maint_api_url = maint_api_url
        self.max_connect_attempts = max_connect_attempts
        self.new_partition_name_date = new_partition_name_date
        self.new_partition_timestamp = new_partition_timestamp
        self.oldest_kept_partition_name_date = oldest_kept_partition_name_date

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        HttpMaint.__init__(self)



class GetDBDataJson(HttpMaint):


    def __init__(self, start_delay = None, ip_list = None, maint_api_url = None, max_connect_attempts = None, table_label = None, id_range = None, config_filepath = None, config_filename = None):

        self.start_delay = start_delay
        self.ip_list = ip_list
        self.maint_api_url = maint_api_url
        self.max_connect_attempts = max_connect_attempts
        self.table_label = table_label
        self.id_range = id_range

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        HttpMaint.__init__(self)



class DirectUpload(HttpHost, HttpClient):


    def __init__(self, channels = None, start_delay = None, ip_list = None, host_api_url = None, client_api_url = None, max_connect_attempts = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.ip_list = ip_list
        self.host_api_url = host_api_url
        self.client_api_url = client_api_url
        self.max_connect_attempts = max_connect_attempts

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        HttpClient.__init__(self)
        HttpHost.__init__(self)



class Replicate(HttpHost):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, ip_list = None, host_api_url = None, max_connect_attempts = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.host_api_url = host_api_url
        self.max_connect_attempts = max_connect_attempts

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        HttpHost.__init__(self)

        self.sql = db.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)

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

                return_string = None

                try:

                    self.sql.connect_db()

                    return_string = ""

                    channel_start = 0
                    end = 0
                    if data_string is not None:
                        end = len(data_string)

                    channel_list = []
                    timestamp_list = []
                    if data_string is not None:
                        channel_timestamps = [channel_string.split(',') for channel_string in data_string.split(';')]
                        channel_list = channel_timestamps[0::2][:-1]
                        timestamp_list = channel_timestamps[1::2]

                    for channel_index in range(len(channel_list)):

                        requested_timestamps = [int(ts_string) for ts_string in timestamp_list[channel_index][:-1]] 
                        channel_string = channel_list[channel_index][0]

                        if not requested_timestamps :
                            pass
                        else :
                            sql_timestamps = '(' + ','.join([str(ts) for ts in requested_timestamps]) + ')'

                            sql_get_values = "SELECT AD.ACQUIRED_TIME,AD.ACQUIRED_VALUE,AD.ACQUIRED_SUBSAMPLES,AD.ACQUIRED_BASE64 FROM t_acquired_data AD WHERE AD.CHANNEL_INDEX=" + channel_string + " AND AD.ACQUIRED_TIME IN " + sql_timestamps
                            rt.logging.debug("sql_get_values",sql_get_values)

                            return_string += channel_string + ';'
                            with self.sql.conn.cursor() as cursor :
                                try:
                                    cursor.execute(sql_get_values)
                                except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                                    rt.logging.exception(e)
                                results = cursor.fetchall()
                                for row in results:
                                    acquired_time = row[0]
                                    acquired_value = row[1]
                                    acquired_subsamples = row[2]
                                    acquired_base64 = row[3]
                                    base64_string = acquired_base64.decode('utf-8')
                                    rt.logging.debug("Channel: ", channel_string, ", Value: ", acquired_value, ", Timestamp: ", acquired_time, ", Sub-samples: ", acquired_subsamples, ", base64: ", acquired_base64)
                                    if acquired_time in requested_timestamps:
                                        requested_timestamps.remove(acquired_time)
                                    return_string += str(acquired_time) + ',' + str(acquired_value) + ',' + str(acquired_subsamples) + ',' + str(base64_string) + ','
                            return_string += ';'

                            if len(requested_timestamps) > 0:
                                if min(requested_timestamps) < int(time.time()) - 3600:
                                    r_clear = self.clear_data_requests(ip)

                except (pymysql.err.OperationalError, pymysql.err.Error) as e:
                    rt.logging.exception(e)

                finally:

                    self.sql.close_db_connection()

                    rt.logging.debug("return_string", return_string)
                    if return_string is not None :
                        r_post = self.set_requested(return_string, ip)


            time.sleep(1.0)



class Download(HttpClient):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, ip_list = None, client_api_url = None, max_connect_attempts = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.client_api_url = client_api_url
        self.max_connect_attempts = max_connect_attempts

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        HttpClient.__init__(self)

        self.sql = db.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)


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

                rt.logging.debug("data_string",data_string)

                channel_data = [channel_string.split(',') for channel_string in data_string.split(';')]
                channel_list = channel_data[0::4][:-1]
                data_list = channel_data[1::4]
                times_list = [data[0::4][:-1] for data in data_list]
                values_list = [data[1::4] for data in data_list]

                try:

                    self.sql.connect_db()

                    for channel, times, values in zip(channel_list, times_list, values_list) :

                        channel= channel[0]

                        for timestamp, value in zip(times, values) :

                            with self.sql.conn.cursor() as cursor :
                                # TODO: precede by SELECT to avoid INSERT attempts causing primary key violations
                                sql = "INSERT INTO t_acquired_data (ACQUIRED_TIME,CHANNEL_INDEX,ACQUIRED_VALUE,STATUS) VALUES (" + str(timestamp) + "," + str(channel) + "," + str(value) + ",0)"
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

            time.sleep(1.0)



class Udp(Uplink):


    def __init__(self) :

        Uplink.__init__(self)

        self.times = None
        self.values = None
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        self.sql = db.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)


    def set_requested(self, channels, times, values, ip = '127.0.0.1'):

        for i in range(len(values)) :

            try :

                data_bytes = struct.pack('>HIf', int(channels[i]), int(times[i]), float(values[i]))   # short unsigned int, big endian
                rt.logging.debug(int(channels[i]), int(times[i]), float(values[i]), ip, self.port)
                try :
                    self.socket.sendto(data_bytes, (ip, self.port))
                except Exception as e:
                    rt.logging.exception('Exception', e)

            except Exception as e:
                rt.logging.exception('Exception', e)


    def run(self):

        time.sleep(self.start_delay)

        while True:

            for ip in self.ip_list :

                acquired_value = None

                try:

                    conn = self.sql.connect_db()

                    self.values = []
                    self.times = []

                    for channel in self.channels :

                        current_timestamp = int(time.time())
                        back_timestamp = current_timestamp - self.max_age

                        sql_get_values = "SELECT AD.ACQUIRED_TIME,AD.ACQUIRED_VALUE,AD.ACQUIRED_SUBSAMPLES,AD.ACQUIRED_BASE64 FROM t_acquired_data AD WHERE AD.CHANNEL_INDEX=" + str(channel) + " AND AD.ACQUIRED_TIME BETWEEN " + str(back_timestamp) + " AND " + str(current_timestamp) + " ORDER BY AD.ACQUIRED_TIME DESC" 
                        rt.logging.debug("sql_get_values",sql_get_values)

                        with conn.cursor() as cursor :
                            try:
                                cursor.execute(sql_get_values)
                            except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                                rt.logging.exception(e)
                            results = cursor.fetchall()
                            rt.logging.debug(results)
                            if len(results) > 0 :
                                row = results[0]
                                acquired_time = row[0]
                                acquired_value = row[1]
                                acquired_subsamples = row[2]
                                acquired_base64 = row[3]
                                base64_string = ''
                                if acquired_base64 is not None :
                                    base64_string = acquired_base64.decode('utf-8')
                                rt.logging.debug("Channel: ", str(channel), ", Value: ", acquired_value, ", Timestamp: ", acquired_time, ", Sub-samples: ", acquired_subsamples, ", base64: ", acquired_base64)
                                self.times.append(acquired_time)
                                self.values.append(acquired_value)

                    rt.logging.debug(list(self.channels), self.times, self.values, ip)
                    self.set_requested(list(self.channels), self.times, self.values, ip)

                except (pymysql.err.OperationalError, pymysql.err.Error) as e:
                    rt.logging.exception(e)

                finally:

                    self.sql.close_db_connection()


            time.sleep(1.0)



class UdpRaw(Udp):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, ip_list = None, port = None, max_age = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.port = port
        self.max_age = max_age

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        Udp.__init__(self)



class UdpNmea(Udp):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, ip_list = None, port = None, nmea_prepend = None, nmea_append = None, max_age = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.port = port
        self.nmea_prepend = nmea_prepend
        self.nmea_append = nmea_append
        self.max_age = max_age

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        Udp.__init__(self)


    def set_requested(self, channels, times, values, ip = '127.0.0.1'):

        for value in values :

            try:
                nmea_data = self.nmea_prepend
                try :
                    nmea_data += "{:.{}f}".format(float(value) / 1.0, 2) 
                except ValueError as e :
                    nmea_data += '9999.0'
                    rt.logging.exception(e)
                finally :
                    nmea_data += self.nmea_append
                rt.logging.debug(nmea_data)

                nmea_string = '$' + nmea_data + '*' + u.nmea_checksum(nmea_data) + '\n'
                self.socket.sendto(nmea_string.encode('utf-8'), (ip, self.port))

            except Exception as e:
                rt.logging.exception(e)



class UdpNmeaPos(Udp) :


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, ip_list = None, port = None, nmea_prepend = None, nmea_append = None, max_age = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.port = port
        self.nmea_prepend = nmea_prepend
        self.nmea_append = nmea_append
        self.max_age = max_age

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        Udp.__init__(self)


    def set_requested(self, channels, times, values, ip = '127.0.0.1'):

        try :

            nmea_data = self.nmea_prepend

            try :

                latitude_dir = 'N'
                latitude = float(values[0])
                latitude_abs = abs(latitude)
                latitude_sign = latitude / latitude_abs
                if latitude_sign < 0 : latitude_dir = 'S'
                latitude_deg = latitude_abs // 1
                latitude_min = ( latitude_abs - latitude_deg ) * 60

                longitude_dir = 'E'
                longitude = float(values[1])
                longitude_abs = abs(longitude)
                longitude_sign = longitude / longitude_abs
                if longitude_sign < 0 : longitude_dir = 'W'
                longitude_deg = longitude_abs // 1
                longitude_min = ( longitude_abs - longitude_deg ) * 60

                datetime_origin = datetime.datetime.fromtimestamp(int(times[0]))
                origin_timestamp = datetime_origin.timetuple()
                hour = origin_timestamp.tm_hour
                min = origin_timestamp.tm_min
                sec = origin_timestamp.tm_sec
                lead_zero = ''
                if longitude_deg < 100 :
                    lead_zero = '0'
                timestamp_string = "{:.{}f}".format(hour * 10000 + min * 100 + sec, 2)
                nmea_data += "{:.{}f}".format(latitude_deg * 100 + latitude_min, 7) + ',' + latitude_dir + ',' + lead_zero + "{:.{}f}".format(longitude_deg * 100 + longitude_min, 7) + ',' + longitude_dir + ',' + timestamp_string

            except ValueError as e :
                nmea_data += '9999.0,N,9999.0,E'
                rt.logging.exception(e)
            finally :
                nmea_data += self.nmea_append

            nmea_string = '$' + nmea_data + '*' + u.nmea_checksum(nmea_data) + '\n'
            rt.logging.debug('nmea_string', nmea_string)
            self.socket.sendto(nmea_string.encode('utf-8'), (ip, self.port))

        except Exception as e:
            rt.logging.exception(e)

            

class UdpAivdmStatic(Udp):


    def __init__(self) :

        Udp.__init__(self)


    def get_aivdm_stat_payload(self) :

        aivdm_payload = ''

        try :

            rt.logging.debug("self.mmsi", self.mmsi, "self.call_sign", self.call_sign, "self.vessel_name", self.vessel_name, "self.ship_type", self.ship_type)
            aivdm_message = ai.AISStaticAndVoyageReportMessage(mmsi = self.mmsi, callsign = self.call_sign, shipname = self.vessel_name, shiptype = self.ship_type, imo = 0)
            aivdm_instance = ai.AIS(aivdm_message)
            aivdm_payload += aivdm_instance.build_payload(False)
            rt.logging.debug("aivdm_payload", aivdm_payload)

        except ValueError as e :

            aivdm_payload += ''
            rt.logging.exception(e)

        finally :

            aivdm_payload += ''

        return aivdm_payload


    # def set_requested(self, channels, times, values, ip = '127.0.0.1'):

        # try :
            # rt.logging.debug("list(channels)", list(channels), "times", times, "values", values, "ip", ip)
            # aivdm_payload = self.get_aivdm_payload()
            # rt.logging.debug('aivdm_payload', aivdm_payload)
            # self.socket.sendto(aivdm_payload.encode('utf-8'), (ip, self.port))

        # except Exception as e:
            # rt.logging.exception(e)



class UdpAivdmPosition(UdpAivdmStatic):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, ip_list = None, port = None, max_age = None, mmsi = None, vessel_name = None, call_sign = None, ship_type = None, nav_status = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
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

        UdpAivdmStatic.__init__(self)


    def get_aivdm_pos_payload(self, timestamp, latitude, longitude) :

        aivdm_payload = ''

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
            
            nav_status = 15
            if self.nav_status is not None : nav_status = self.nav_status

            aivdm_message = ai.AISPositionReportMessage(mmsi = self.mmsi, lon = longitude_min_fraction, lat = latitude_min_fraction, ts = sec, status = nav_status)
            aivdm_instance = ai.AIS(aivdm_message)
            aivdm_payload += aivdm_instance.build_payload(False)
            rt.logging.debug("aivdm_payload", aivdm_payload)

        except ValueError as e :

            aivdm_payload += ''
            rt.logging.exception(e)

        finally :

            aivdm_payload += ''

        return aivdm_payload


    def set_requested(self, channels, times, values, ip = '127.0.0.1') :

        try :
            aivdm_stat_payload = self.get_aivdm_stat_payload()
            rt.logging.debug('aivdm_stat_payload', aivdm_stat_payload)
            self.socket.sendto(aivdm_stat_payload.encode('utf-8'), (ip, self.port))
            rt.logging.debug("list(channels)", list(channels), "times", times, "values", values, "ip", ip)
            aivdm_pos_payload = self.get_aivdm_pos_payload(times[0], values[0], values[1])
            rt.logging.debug('aivdm_pos_payload', aivdm_pos_payload)
            self.socket.sendto(aivdm_pos_payload.encode('utf-8'), (ip, self.port))
        except Exception as e:
            rt.logging.exception(e)



class BinaryBroadcastMessageAreaNoticeCircle(Udp):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, ip_list = None, port = None, max_age = None, mmsi = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.port = port
        self.max_age = max_age

        self.mmsi = mmsi

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        Udp.__init__(self)


    def get_aivdm_area_notice_circle_payload(self, timestamp, latitude, longitude) :

        aivdm_payload = ''

        try :

            latitude_degs = float(latitude)
            latitude_min_fraction = int(latitude_degs * 60 * 1000)
            rt.logging.debug("latitude_degs", latitude_degs, "latitude_min_fraction", latitude_min_fraction)

            longitude_degs = float(longitude)
            longitude_min_fraction = int(longitude_degs * 60 * 1000)
            rt.logging.debug("longitude_degs", longitude_degs, "longitude_min_fraction", longitude_min_fraction)

            #datetime_origin = datetime.datetime.fromtimestamp(int(timestamp))
            #origin_timestamp = datetime_origin.timetuple()
            #sec = origin_timestamp.tm_sec

            rt.logging.debug("self.mmsi", self.mmsi)
            aivdm_message = ai.AISBinaryBroadcastMessageAreaNoticeCircle(mmsi = self.mmsi, lon_1 = longitude_min_fraction, lat_1 = latitude_min_fraction, radius_1 = 100)
            aivdm_instance = ai.AIS(aivdm_message)
            aivdm_payload += aivdm_instance.build_payload(False)
            rt.logging.debug("aivdm_payload", aivdm_payload)

        except ValueError as e :

            aivdm_payload += ''
            rt.logging.exception(e)

        finally :

            aivdm_payload += ''

        return aivdm_payload


    def set_requested(self, channels, times, values, ip = '127.0.0.1') :

        try :
            rt.logging.debug("list(channels)", list(channels), "times", times, "values", values, "ip", ip)
            aivdm_area_notice_circle_payload = self.get_aivdm_area_notice_circle_payload(times[0], values[0], values[1])
            rt.logging.debug('aivdm_area_notice_circle_payload', aivdm_area_notice_circle_payload)
            self.socket.sendto(aivdm_area_notice_circle_payload.encode('utf-8'), (ip, self.port))
        except Exception as e:
            rt.logging.exception(e)



class AtonReport(Udp):

    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, ip_list = None, port = None, max_age = None, length_offset = None, width_offset = None, mmsi = None, aid_type = None, name = None, virtual_aid = None, config_filepath = None, config_filename = None) :
        #repeat = 0, mmsi = 0, aid_type = 0, name = 0, accuracy = 0, lon = 181000, lat = 91000, to_bow = 0, to_stern = 0, to_port = 0, to_starboard = 0, epfd = 0, ts = 60, off_position = 0, raim = 0, virtual_aid = 0, assigned = 0)
        self.channels = channels
        self.start_delay = start_delay
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

        Udp.__init__(self)


    def get_aivdm_aton_payload(self, timestamp, latitude, longitude) :

        aivdm_payloads = []

        try :

            latitude_degs = float(latitude)

            latitude_factor = math.cos(latitude_degs/180 * math.pi)
            rt.logging.debug("self.mmsi", self.mmsi, "self.length_offset", self.length_offset, "self.width_offset", self.width_offset)

            #for i in range(0,len(self.mmsi)) :
            for mmsi, aid_type, name, virtual_aid, length_offset, width_offset in zip(self.mmsi, self.aid_type, self.name, self.virtual_aid, self.length_offset, self.width_offset) :

                latitude_degs = float(latitude)
                longitude_degs = float(longitude)

                latitude_degs += float ( length_offset / ( 1 * 1852 ) * 1/60 )
                latitude_min_fraction = int(latitude_degs * 60 * 10000)
                rt.logging.debug("latitude_degs", latitude_degs, "latitude_min_fraction", latitude_min_fraction)

                longitude_degs += float ( width_offset / ( latitude_factor * 1852 ) * 1/60 )
                longitude_min_fraction = int(longitude_degs * 60 * 10000)
                rt.logging.debug("longitude_degs", longitude_degs, "longitude_min_fraction", longitude_min_fraction)

                #datetime_origin = datetime.datetime.fromtimestamp(int(timestamp))
                #origin_timestamp = datetime_origin.timetuple()
                #sec = origin_timestamp.tm_sec

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


    def set_requested(self, channels, times, values, ip = '127.0.0.1') :

        try :

            rt.logging.debug("list(channels)", list(channels), "times", times, "values", values, "ip", ip)
            aivdm_aton_payloads = self.get_aivdm_aton_payload(times[0], values[0], values[1])
            rt.logging.debug('aivdm_aton_payloads', aivdm_aton_payloads)

            for aivdm_aton_payload in aivdm_aton_payloads :
                self.socket.sendto(aivdm_aton_payload.encode('utf-8'), (ip, self.port))

        except Exception as e :

            rt.logging.exception(e)
