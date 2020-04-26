#

import requests
import time
import json
import pymysql
import socket
import http

import runtime
import metadata



class Host:


    def __init__(self):
        pass



class Http:
    

    def __init__(self, channels = None, start_delay = 0, ip_list = ['0.0.0.0'], max_connect_attempts = 10):

        self.channels = channels
        self.start_delay = start_delay
        self.ip_list = ip_list
        self.max_connect_attempts = max_connect_attempts

        self.config = metadata.Configure()
        self.env = self.config.get()

        self.CHANNEL_RANGE_STRING = ''
        for ch in channels: 
            self.CHANNEL_RANGE_STRING +=  str(ch) + ';;'

        self.connect_attempts = 0
          

    def get_requested(self, channels, ip):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            runtime.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            runtime.logging.debug("channels", channels)
            raw_data = requests.post('http://' + ip + '/host/get_requested.php', timeout=5, data={'channelrange': channels, 'duration': 10, 'unit': 1})
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            runtime.logging.debug(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.get_requested(channels, ip)
            else:
                exit(-1)


    def set_requested(self, data_string, ip):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            runtime.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            raw_data = requests.post('http://' + ip + '/host/set_requested.php', timeout=5, data={'returnstring': data_string})
            runtime.logging.debug("data_string", data_string)
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            runtime.logging.debug(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.set_requested(data_string, ip)
            else:
                exit(-1)

                
    def clear_data_requests(self, channels, ip):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            runtime.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            runtime.logging.debug("channels", channels)
            raw_data = requests.post('http://' + ip + '/host/clear_data_requests.php', timeout=5, data={'channelrange': channels})
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            runtime.logging.debug(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.clear_data_requests(channels, ip)
            else:
                exit(-1)


    def clear_channels(self):

        for ip in self.ip_list :

            cleared_channels = ""
            r_clear = self.clear_data_requests(self.CHANNEL_RANGE_STRING, ip)
            runtime.logging.debug("r_clear", r_clear)
            if r_clear is not None:
                try:
                    requested_data = r_clear.json()
                    cleared_channels = requested_data['returnstring']
                except ValueError as e:  # includes simplejson.decoder.JSONDecodeError
                    runtime.logging.debug('Decoding JSON has failed', e)
            runtime.logging.debug("cleared_channels",cleared_channels)


    def run(self):

        while True:

            for ip in self.ip_list :

                data_string = ""
                r_get = self.get_requested(self.CHANNEL_RANGE_STRING, ip)
                runtime.logging.debug("r_get", r_get)
                if r_get is not None:
                    try:
                        requested_data = r_get.json()
                        data_string = requested_data['returnstring']
                    except ValueError as e:  # includes simplejson.decoder.JSONDecodeError
                        runtime.logging.debug('Decoding JSON has failed', e)
                runtime.logging.debug("data_string",data_string)

                try:

                    conn = pymysql.connect(host=self.env['STORE_DATABASE_HOST'], user=self.env['STORE_DATABASE_USER'], passwd=self.env['STORE_DATABASE_PASSWD'], db=self.env['STORE_DATABASE_DB'], autocommit=True)

                    return_string = ""

                    channel_start = 0
                    end = 0
                    if data_string is not None:
                        end = len(data_string)

                    while channel_start < end:

                        channel_end = data_string.find(";", channel_start, end)
                        channel_string = data_string[channel_start:channel_end]
                        channel = int(channel_string)

                        points_start = channel_end+1
                        end_position = len(data_string)
                        points_end = data_string.find(";", points_start, end_position)
                        timestamp_start = points_start
                        requested_timestamps = []

                        if timestamp_start < points_end - 1:
                            while timestamp_start < points_end - 1:
                                timestamp_end = data_string.find(",", timestamp_start, points_end)
                                timestamp_string = data_string[timestamp_start:timestamp_end]
                                timestamp = int(timestamp_string)
                                requested_timestamps.append(timestamp)
                                timestamp_start = timestamp_end + 1
                        else :
                            timestamp_end = timestamp_start - 1

                        channel_start = timestamp_end + 2

                        if not requested_timestamps :
                            pass
                        else :
                            sql_timestamps = "("
                            for timestamp in requested_timestamps[:-1] :
                                sql_timestamps += str(timestamp) + ","
                            sql_timestamps += str(requested_timestamps[-1])
                            sql_timestamps += ")"

                            sql_get_values = "SELECT AD.ACQUIRED_TIME,AD.ACQUIRED_VALUE,AD.ACQUIRED_SUBSAMPLES,AD.ACQUIRED_BASE64 FROM t_acquired_data AD WHERE AD.CHANNEL_INDEX=" + channel_string + " AND AD.ACQUIRED_TIME IN " + sql_timestamps
                            runtime.logging.debug("sql_get_values",sql_get_values)
                            return_string += channel_string + ";"
                            with conn.cursor() as cursor :
                                try:
                                    cursor.execute(sql_get_values)
                                except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                                    runtime.logging.debug(e)
                                results = cursor.fetchall()
                                for row in results:
                                    acquired_time = row[0]
                                    acquired_value = row[1]
                                    acquired_subsamples = row[2]
                                    acquired_base64 = row[3]
                                    base64_string = acquired_base64.decode('utf-8')
                                    runtime.logging.debug('Channel: ', channel, ', Value: ', acquired_value, ', Timestamp: ', acquired_time, ', Sub-samples: ', acquired_subsamples, ', base64: ', acquired_base64)
                                    if acquired_time in requested_timestamps:
                                        requested_timestamps.remove(acquired_time)
                                    return_string += str(acquired_time) + "," + str(acquired_value) + "," + str(acquired_subsamples) + "," + str(base64_string) + ","
                            return_string += ";"

                            if len(requested_timestamps) > 0:
                                if min(requested_timestamps) < int(time.time()) - 3600:
                                    r_clear = self.clear_data_requests(str(channel) + ';;', ip)

                        channel_start = timestamp_end + 2

                except (pymysql.err.OperationalError, pymysql.err.Error) as e:
                    runtime.logging.debug(e)

                finally:

                    try:
                        conn.close()
                    except pymysql.err.Error as e:
                        runtime.logging.debug(e)

                    runtime.logging.debug("return_string", return_string)

                    r_post = self.set_requested(return_string, ip)


            time.sleep(1.0)
