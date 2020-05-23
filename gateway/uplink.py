#

import requests
import time
import json
import pymysql
import socket
import http

import gateway.runtime as rt
import gateway.metadata as md


class Task:


    def __init__(self):

        self.env = self.get_env()

        
    def get_env(self): 

        config = md.Configure(filepath = self.config_filepath, filename = self.config_filename)
        env = config.get()

        return env



class HttpTask(Task):
    

    def __init__(self, channels = None, start_delay = 0, ip_list = ['0.0.0.0'], max_connect_attempts = 10, config_filepath = None, config_filename = None):

        self.config_filepath = config_filepath
        self.config_filename = config_filename
        self.channels = channels
        self.start_delay = start_delay
        self.ip_list = ip_list
        self.max_connect_attempts = max_connect_attempts

        Task.__init__(self)
        
        self.CHANNEL_RANGE_STRING = ''
        for ch in channels: 
            self.CHANNEL_RANGE_STRING +=  str(ch) + ';;'
        
        self.connect_attempts = 0
        
        self.clear_channels()
        
         
    def get_requested(self, channels, ip):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            rt.logging.debug("channels", channels)
            raw_data = requests.post('http://' + ip + '/host/get_requested.php', timeout=5, data={'channelrange': channels, 'duration': 10, 'unit': 1})
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.debug(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.get_requested(channels, ip)
            else:
                exit(-1)


    def set_requested(self, data_string, ip):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            raw_data = requests.post('http://' + ip + '/host/set_requested.php', timeout=5, data={'returnstring': data_string})
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

                
    def clear_data_requests(self, channels, ip):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            rt.logging.debug("channels", channels)
            raw_data = requests.post('http://' + ip + '/host/clear_data_requests.php', timeout=5, data={'channelrange': channels})
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.debug(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.clear_data_requests(channels, ip)
            else:
                exit(-1)


    def clear_channels(self):

        for ip in self.ip_list :

            cleared_channels = ""
            r_clear = self.clear_data_requests(self.CHANNEL_RANGE_STRING, ip)
            rt.logging.debug("r_clear", r_clear)
            if r_clear is not None:
                try:
                    requested_data = r_clear.json()
                    cleared_channels = requested_data['returnstring']
                except ValueError as e:  # includes simplejson.decoder.JSONDecodeError
                    rt.logging.debug('Decoding JSON has failed', e)
            rt.logging.debug("cleared_channels",cleared_channels)


    def run(self):

        while True:

            for ip in self.ip_list :

                data_string = ""
                r_get = self.get_requested(self.CHANNEL_RANGE_STRING, ip)
                rt.logging.debug("r_get", r_get)
                if r_get is not None:
                    try:
                        requested_data = r_get.json()
                        data_string = requested_data['returnstring']
                    except ValueError as e:  # includes simplejson.decoder.JSONDecodeError
                        rt.logging.debug('Decoding JSON has failed', e)
                rt.logging.debug("data_string",data_string)

                try:

                    conn = pymysql.connect(host=self.env['STORE_DATABASE_HOST'], user=self.env['STORE_DATABASE_USER'], passwd=self.env['STORE_DATABASE_PASSWD'], db=self.env['STORE_DATABASE_DB'], autocommit=True)

                    return_string = ""

                    channel_start = 0
                    end = 0
                    if data_string is not None:
                        end = len(data_string)

                    channel_list = []
                    timestamp_list = []
                    if data_string is not None:
                        channel_timestamps = [channel_string.split(",") for channel_string in data_string.split(";")]
                        channel_list = channel_timestamps[0::2][:-1]
                        timestamp_list = channel_timestamps[1::2]
                        
                    for channel_index in range(len(channel_list)):
                        #print(channel_list[channel_index][0])
                        #print(timestamp_list[channel_index][:-1])

                        requested_timestamps = [int(timestamp_string) for timestamp_string in timestamp_list[channel_index][:-1]] 
                        #print(requested_timestamps)
                        channel_string = channel_list[channel_index][0]
                        #print(channel_string)

                        if not requested_timestamps :
                            pass
                        else :
                            sql_timestamps = "("
                            for timestamp in requested_timestamps[:-1] :
                                sql_timestamps += str(timestamp) + ","
                            sql_timestamps += str(requested_timestamps[-1])
                            sql_timestamps += ")"

                            sql_get_values = "SELECT AD.ACQUIRED_TIME,AD.ACQUIRED_VALUE,AD.ACQUIRED_SUBSAMPLES,AD.ACQUIRED_BASE64 FROM t_acquired_data AD WHERE AD.CHANNEL_INDEX=" + channel_string + " AND AD.ACQUIRED_TIME IN " + sql_timestamps
                            rt.logging.debug("sql_get_values",sql_get_values)
                            #print("sql_get_values",sql_get_values)
                            return_string += channel_string + ";"
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
                                    rt.logging.debug('Channel: ', channel_string, ', Value: ', acquired_value, ', Timestamp: ', acquired_time, ', Sub-samples: ', acquired_subsamples, ', base64: ', acquired_base64)
                                    if acquired_time in requested_timestamps:
                                        requested_timestamps.remove(acquired_time)
                                    return_string += str(acquired_time) + "," + str(acquired_value) + "," + str(acquired_subsamples) + "," + str(base64_string) + ","
                            return_string += ";"

                            if len(requested_timestamps) > 0:
                                if min(requested_timestamps) < int(time.time()) - 3600:
                                    r_clear = self.clear_data_requests(channel_string + ';;', ip)

                except (pymysql.err.OperationalError, pymysql.err.Error) as e:
                    rt.logging.debug(e)

                finally:

                    try:
                        conn.close()
                    except pymysql.err.Error as e:
                        rt.logging.debug(e)

                    rt.logging.debug("return_string", return_string)

                    r_post = self.set_requested(return_string, ip)


            time.sleep(1.0)
