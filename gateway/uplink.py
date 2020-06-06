#

import requests
import time
import json
import pymysql
import socket
import http

import gateway.runtime as rt
import gateway.metadata as md
import gateway.task as t



class Uplink(t.StoreUplink):


    def __init__(self):

        self.env = self.get_env()
        if self.ip_list is None: self.ip_list = self.env['IP_LIST']
        if self.host_api_url is None: self.host_api_url = self.env['HOST_API_URL']
        if self.max_connect_attempts is None: self.max_connect_attempts = self.env['MAX_CONNECT_ATTEMPTS']
        
        t.StoreUplink.__init__(self)



class Http(Uplink):
    

    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, ip_list = None, host_api_url = None, client_api_url = None, max_connect_attempts = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.host_api_url = host_api_url
        self.client_api_url = client_api_url
        self.max_connect_attempts = max_connect_attempts

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        Uplink.__init__(self)
        
        self.channel_range_string = ';;'.join([str(ch) for ch in channels]) + ';;'

        self.connect_attempts = 0


    def get_requested(self, ip = '127.0.0.1'):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            rt.logging.debug("self.channel_range_string", self.channel_range_string)
            raw_data = requests.post("http://" + ip + self.host_api_url + "get_requested.php", timeout = 5, data = {"channelrange": self.channel_range_string, "duration": 10, "unit": 1})
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.debug(e)
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
            rt.logging.debug(e)
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
            rt.logging.debug("self.channel_range_string", self.channel_range_string)
            raw_data = requests.post("http://" + ip + self.host_api_url + "clear_data_requests.php", timeout = 5, data = {"channelrange": self.channel_range_string})
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.debug(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.clear_data_requests(ip)
            else:
                exit(-1)

                
    def send_request(self, ip = '127.0.0.1', start_time = -9999, end_time = -9999, duration = 10, unit = 1, delete_horizon = 3600):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            d = {"channels": self.channel_range_string, "start_time": start_time, "end_time": end_time, "duration": duration, "unit": unit, "delete_horizon": delete_horizon}
            raw_data = requests.post("http://" + ip + self.client_api_url + "send_request.php", timeout = 5, data = d)
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.debug(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.send_request(ip, start_time, end_time, duration, unit, delete_horizon)
            else:
                exit(-1)

            
            
class Replicate(Http):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, ip_list = None, host_api_url = None, client_api_url = None, max_connect_attempts = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.ip_list = ip_list
        self.host_api_url = host_api_url
        self.client_api_url = client_api_url
        self.max_connect_attempts = max_connect_attempts

        self.config_filepath = config_filepath
        self.config_filename = config_filename
        
        self.env = self.get_env()

        Http.__init__(self, self.channels, self.start_delay, self.gateway_database_connection, self.ip_list, self.host_api_url, self.client_api_url, self.max_connect_attempts, self.config_filepath, self.config_filename)

        self.connect_attempts = 0
        
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

                conn = None 
                return_string = None

                try:
                    conn_data = self.gateway_database_connection
                    conn = pymysql.connect(host = conn_data['host'], user = conn_data['user'], passwd = conn_data['passwd'], db = conn_data['db'], autocommit = True)

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
                            with conn.cursor() as cursor :
                                try:
                                    cursor.execute(sql_get_values)
                                except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                                    rt.logging.debug(e)
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
                    rt.logging.debug(e)

                finally:

                    try:
                        if conn is not None: conn.close()
                    except pymysql.err.Error as e:
                        rt.logging.debug(e)

                    rt.logging.debug("return_string", return_string)

                    r_post = self.set_requested(return_string, ip)


            time.sleep(1.0)
