import requests
import time
import json
import pymysql
import socket
import http


def clear_data_requests(channels):
    global connection_attempts
    connection_attempts += 1
    if connection_attempts > 1:
        print("Retrying connection, attempt " + str(connection_attempts))
    try:
        print("channels", channels)
        raw_data = requests.post("http://quf.se/logging/upload/clear_data_requests.php", {'channelrange': channels})
        connection_attempts = 0
        return raw_data
    except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
        print(e)
        time.sleep(10)
        if connection_attempts < 50:
            clear_data_requests(channels)
        else:
            exit(-1)
    
def get_control_requests(channels):
    global connection_attempts
    connection_attempts += 1
    if connection_attempts > 1:
        print("Retrying connection, attempt " + str(connection_attempts))
    try:
        print("channels", channels)
        raw_data = requests.post("http://quf.se/logging/upload/get_control_requests.php", {'channelrange': channels})
        connection_attempts = 0
        return raw_data
    except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
        print(e)
        time.sleep(10)
        if connection_attempts < 50:
            get_control_requests(channels)
        else:
            exit(-1)

def set_control_requests(channels):
    global connection_attempts
    connection_attempts += 1
    if connection_attempts > 1:
        print("Retrying connection, attempt " + str(connection_attempts))
    try:
        raw_data = requests.post("http://quf.se/logging/upload/set_control_requests.php", {'returnstring': data_string})
        print("data_string", data_string)
        connection_attempts = 0
        return raw_data
    except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
        print(e)
        time.sleep(10)
        if connection_attempts < 50:
            set_control_requests(data_string)
        else:
            exit(-1)
    

CONTROL_CHANNEL_RANGE = "39;;"

connection_attempts = 0

cleared_channels = ""
r_clear = clear_data_requests(CONTROL_CHANNEL_RANGE)
print("r_clear", r_clear)
if r_clear is not None:
    try:
        requested_data = r_clear.json()
        cleared_channels = requested_data['returnstring']
    except ValueError as e:  # includes simplejson.decoder.JSONDecodeError
        print('Decoding JSON has failed', e)
print("cleared_channels",cleared_channels)


while True:

    data_string = ""

    r_get = get_control_requests(CONTROL_CHANNEL_RANGE)
    print("r_get", r_get)
    if r_get is not None:
        try:
            requested_data = r_get.json()
            data_string = requested_data['returnstring']
        except ValueError as e:  # includes simplejson.decoder.JSONDecodeError
            print('Decoding JSON has failed', e)
    print("data_string",data_string)

    channel_start = 0
    end = 0
    if data_string is not None:
        end = len(data_string)


    try:

        conn = pymysql.connect(host='localhost', user='root', passwd='root', db='test', autocommit=True)

        while channel_start < end:
            
            insert_failure = False

            channel_end = data_string.find(";", channel_start, end)
            channel_string = data_string[channel_start:channel_end]
            #channel = int(channel_string)

            points_start = channel_end+1
            points_end = data_string.find(";", points_start, end)

            timestamp_start = points_start
            
            while timestamp_start < points_end - 1:
                timestamp_end = data_string.find(",", timestamp_start, points_end)
                timestamp_string = data_string[timestamp_start:timestamp_end]
                print("timestamp_string",timestamp_string)
                value_start = timestamp_end+1
                value_end = data_string.find(",", value_start, points_end)
                value_string = data_string[value_start:value_end]
                print("value_string", value_string)
                timestamp_start = value_end+1

                if value_string != "":
                    with conn.cursor() as cursor :
                        sql = "INSERT INTO T_ACQUIRED_DATA (ACQUIRED_TIME,CHANNEL_INDEX,ACQUIRED_VALUE,STATUS) VALUES (" + timestamp_string + "," + channel_string + "," + value_string + ",-1)"
                        print("print(sql) ", sql)
                        try:
                            cursor.execute(sql)
                        except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                            print(e)
                        result = cursor.rowcount
                        print("result", result)
                        if result <= -1: insert_failure = True

            if not insert_failure:
                conn.commit()
                print('committed')

            channel_start = value_end + 2


    except (pymysql.err.OperationalError, pymysql.err.Error) as e:

        print(e)


    finally:
     
        try:
            conn.close()
        except pymysql.err.Error as e:
            print(e)

        if data_string is not None:
            r_post = set_control_requests(data_string)


    time.sleep(1.0)
