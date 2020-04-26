import requests
import time
import json
import pymysql
import socket
import http

import runtime as rt
import metadata


config = metadata.Configure()
env = config.get()


def get_requested(channels, ip):
    global connection_attempts
    connection_attempts += 1
    if connection_attempts > 1:
        rt.logging.debug("Retrying connection, attempt " + str(connection_attempts))
    try:
        rt.logging.debug("channels", channels)
        raw_data = requests.post('http://' + ip + '/host/get_requested.php', timeout=5, data={'channelrange': channels, 'duration': 10, 'unit': 1})
        connection_attempts = 0
        return raw_data
    except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
        rt.logging.debug(e)
        time.sleep(10)
        if connection_attempts < 50:
            get_requested(channels, ip)
        else:
            exit(-1)

def set_requested(data_string, ip):
    global connection_attempts
    connection_attempts += 1
    if connection_attempts > 1:
        rt.logging.debug("Retrying connection, attempt " + str(connection_attempts))
    try:
        raw_data = requests.post('http://' + ip + '/host/set_requested.php', timeout=5, data={'returnstring': data_string})
        rt.logging.debug("data_string", data_string)
        connection_attempts = 0
        return raw_data
    except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
        rt.logging.debug(e)
        time.sleep(10)
        if connection_attempts < 50:
            set_requested(data_string, ip)
        else:
            exit(-1)

def clear_data_requests(channels, ip):
    global connection_attempts
    connection_attempts += 1
    if connection_attempts > 1:
        rt.logging.debug("Retrying connection, attempt " + str(connection_attempts))
    try:
        rt.logging.debug("channels", channels)
        #print("channels", channels)
        raw_data = requests.post('http://' + ip + '/host/clear_data_requests.php', timeout=5, data={'channelrange': channels})
        #print('raw_data', raw_data)
        connection_attempts = 0
        return raw_data
    except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
        rt.logging.debug(e)
        time.sleep(10)
        if connection_attempts < 50:
            clear_data_requests(channels, ip)
        else:
            exit(-1)

ip_list = ['109.74.8.89']

CHANNEL_RANGE_STRING = "140;;160;;180;;600;;10002;;"

connection_attempts = 0

for ip in ip_list :
    #print(ip)
    cleared_channels = ""
    r_clear = clear_data_requests(CHANNEL_RANGE_STRING, ip)
    rt.logging.debug("r_clear", r_clear)
    if r_clear is not None:
        try:
            requested_data = r_clear.json()
            cleared_channels = requested_data['returnstring']
        except ValueError as e:  # includes simplejson.decoder.JSONDecodeError
            rt.logging.debug('Decoding JSON has failed', e)
    rt.logging.debug("cleared_channels",cleared_channels)


while True:

    for ip in ip_list :
        #print(ip)
        data_string = ""
        r_get = get_requested(CHANNEL_RANGE_STRING, ip)
        rt.logging.debug("r_get", r_get)
        if r_get is not None:
            try:
                requested_data = r_get.json()
                data_string = requested_data['returnstring']
            except ValueError as e:  # includes simplejson.decoder.JSONDecodeError
                rt.logging.debug('Decoding JSON has failed', e)
        rt.logging.debug("data_string",data_string)

        try:

            conn = pymysql.connect(host=env['STORE_DATABASE_HOST'], user=env['STORE_DATABASE_USER'], passwd=env['STORE_DATABASE_PASSWD'], db=env['STORE_DATABASE_DB'], autocommit=True)

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
                    #print(sql_get_values)
                    rt.logging.debug("sql_get_values",sql_get_values)
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
                            rt.logging.debug('Channel: ', channel, ', Value: ', acquired_value, ', Timestamp: ', acquired_time, ', Sub-samples: ', acquired_subsamples, ', base64: ', acquired_base64)
                            #print('Channel: ', channel, ', Value: ', acquired_value, ', Timestamp: ', acquired_time, ', Sub-samples: ', acquired_subsamples, ', base64: ', acquired_base64)
                            if acquired_time in requested_timestamps:
                                requested_timestamps.remove(acquired_time)
                            return_string += str(acquired_time) + "," + str(acquired_value) + "," + str(acquired_subsamples) + "," + str(base64_string) + ","
                    return_string += ";"

                    if len(requested_timestamps) > 0:
                        if min(requested_timestamps) < int(time.time()) - 3600:
                            r_clear = clear_data_requests(str(channel) + ';;', ip)

                channel_start = timestamp_end + 2

        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            rt.logging.debug(e)

        finally:

            try:
                conn.close()
            except pymysql.err.Error as e:
                rt.logging.debug(e)

            rt.logging.debug("return_string", return_string)

            r_post = set_requested(return_string, ip)

            #if r_post is not None:
            #    requested_data = r_post.json()
            #    data_string = requested_data['returnstring']
            #else:
            #    data_string = ""
            #print(data_string)

    time.sleep(1.0)
