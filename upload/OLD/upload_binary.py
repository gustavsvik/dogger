import requests
import time
import json
import pymysql
import socket
import http


def get_requested(channels):
    global connection_attempts
    connection_attempts += 1
    if connection_attempts > 1:
        print("Retrying connection, attempt " + str(connection_attempts))
    try:
        #pprint("channels", channels)
        raw_data = requests.post("http://quf.se/logging/upload/get_requested.php", {'channelrange': channels})
        connection_attempts = 0
        print('raw_data', raw_data)
        return raw_data
    except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
        print(e)
        time.sleep(10)
        if connection_attempts < 50:
            get_requested(channels)
        else:
            exit(-1)

def set_requested(data_string):
    global connection_attempts
    connection_attempts += 1
    if connection_attempts > 1:
        print("Retrying connection, attempt " + str(connection_attempts))
    try:
        raw_data = requests.post("http://quf.se/logging/upload/set_requested.php", {'returnstring': data_string})
        connection_attempts = 0
        print('raw_data', raw_data)
        return raw_data
    except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
        print(e)
        time.sleep(10)
        if connection_attempts < 50:
            set_requested(data_string)
        else:
            exit(-1)

def clear_data_requests(channels):
    global connection_attempts
    connection_attempts += 1
    if connection_attempts > 1:
        print("Retrying connection, attempt " + str(connection_attempts))
    try:
        #pprint("channels", channels)
        raw_data = requests.post("http://quf.se/logging/upload/clear_data_requests.php", {'channelrange': channels})
        connection_attempts = 0
        print('raw_data', raw_data)
        return raw_data
    except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
        print(e)
        time.sleep(10)
        if connection_attempts < 50:
            clear_data_requests(channels)
        else:
            exit(-1)
    

CHANNEL_RANGE_STRING = "140;;160;;600;;"

connection_attempts = 0

cleared_channels = ""
r_clear = clear_data_requests(CHANNEL_RANGE_STRING)
#print("r_clear", r_clear)
if r_clear is not None:
    try:
        requested_data = r_clear.json()
        cleared_channels = requested_data['returnstring']
    except ValueError as e:  # includes simplejson.decoder.JSONDecodeError
        print('Decoding JSON has failed', e)
#print("cleared_channels",cleared_channels)


while True:

    data_string = ""

    r_get = get_requested(CHANNEL_RANGE_STRING)
    #print("r_get", r_get)
    if r_get is not None:
        try:
            requested_data = r_get.json()
            data_string = requested_data['returnstring']
        except ValueError as e:  # includes simplejson.decoder.JSONDecodeError
            print('Decoding JSON has failed', e)
    #print("data_string",data_string)

    try:
                
        conn = pymysql.connect(host='localhost', user='root', passwd='root', db='test', autocommit=True)

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

            sql_timestamps = " ("
            
            while timestamp_start < points_end - 1:
                timestamp_end = data_string.find(",", timestamp_start, points_end)
                timestamp_string = data_string[timestamp_start:timestamp_end]
                timestamp = int(timestamp_string)
                timestamp_start = timestamp_end+1
                #print(channel, timestamp)
                sql_timestamps += timestamp_string
                if timestamp_start < points_end - 1: sql_timestamps += ","

            sql_timestamps += ")"
            sql_get_values = "SELECT AD.ACQUIRED_TIME,AD.ACQUIRED_VALUE,AD.ACQUIRED_SUBSAMPLES,AD.ACQUIRED_BASE64 FROM T_ACQUIRED_DATA AD WHERE AD.CHANNEL_INDEX=" + channel_string + " AND AD.ACQUIRED_TIME IN" + sql_timestamps
            #print("sql_get_values",sql_get_values)

            return_string += channel_string + ";"
            with conn.cursor() as cursor :
                try:
                    cursor.execute(sql_get_values)
                except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                    print(e)
                results = cursor.fetchall()
                for row in results:
                    acquired_time = row[0]
                    acquired_value = row[1]
                    acquired_subsamples = row[2]
                    acquired_base64 = row[3]
                    base64_string = acquired_base64.decode('utf-8')
                    print('Channel: ', channel, ', Value: ', acquired_value, ', Timestamp: ', acquired_time) #, ', Sub-samples: ', acquired_subsamples, ', base64: ', acquired_base64)
                    return_string += str(acquired_time) + "," + str(acquired_value) + "," + str(acquired_subsamples) + "," + str(base64_string) + ","
            return_string += ";"
            
            channel_start = timestamp_end + 2

            #with conn.cursor() as cursor :
            #    UPDATE mytable SET fruit='orange', drink='water', food='pizza' WHERE id=1;
            #    sql = "UPDATE T_ACQUIRED_DATA SET ACQUIRED_VALUE=" +  + ,ACQUIRED_SUBSAMPLES) VALUES (" + acquired_time_string + "," + repr(channel_index) + "," + repr(acquired_value) + ",'" + acquired_subsamples + "')"
            #    cursor.execute(sql)
                         
        #conn.commit()
            
    except (pymysql.err.OperationalError, pymysql.err.Error) as e:
        print(e)

    finally:
                
        try:
            conn.close()
        except pymysql.err.Error as e:
            print(e)
        
        r_post = set_requested(return_string)
        
        #if r_post is not None:
        #    requested_data = r_post.json()
        #    data_string = requested_data['returnstring']
        #else:
        #    data_string = ""
        #print(data_string)

        
    time.sleep(1.0)
