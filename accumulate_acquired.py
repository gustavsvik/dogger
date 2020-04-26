import time
import pymysql

#import configuration
import runtime
from metadata import Configure

config = Configure()
env = config.get()


time.sleep(20)

#env = configuration.get()

channels = [21,23,20,24]
channels.extend(list(range(97, 113)))

previous_minute = -1
previous_timestamp = int(time.mktime(time.localtime()))

while (True):

    time.sleep(0.1)

    current_localtime = time.localtime()
    current_hour =  current_localtime.tm_hour
    current_minute = current_localtime.tm_min
    current_second = current_localtime.tm_sec
    current_timestamp = int(time.mktime(time.localtime()))

    if current_timestamp - previous_timestamp > 60 :
        current_timestamp = previous_timestamp + 60
        current_minute = previous_minute + 1
    
    if current_second == 0 and current_minute != previous_minute :

        for channel_index in channels: #list(range(env['DATA_CHANNEL_1'], env['DATA_CHANNEL_1'] + env['NO_OF_DATA_CHANNELS'])):

            accumulated_min_samples = 0
            accumulated_min_value = 0.0
            accumulated_hour_samples = 0
            accumulated_hour_value = 0.0
            accumulated_text = ''
            accumulated_binary = b''
            
            try :

                conn = pymysql.connect(host = env['STORE_DATABASE_HOST'], user = env['STORE_DATABASE_USER'], passwd = env['STORE_DATABASE_PASSWD'], db = env['STORE_DATABASE_DB'], autocommit = True)

                accumulated_bin_size = 60
                accumulated_bin_end_time = current_timestamp - accumulated_bin_size

                sql_get_minute_data = 'SELECT ACQUIRED_TIME,ACQUIRED_VALUE FROM t_acquired_data WHERE CHANNEL_INDEX=' + str(channel_index) + ' AND ACQUIRED_TIME<' + str(accumulated_bin_end_time) + ' AND ACQUIRED_TIME>=' + str(accumulated_bin_end_time - 60) + ' ORDER BY ACQUIRED_TIME DESC'
                #print('sql_get_minute_data', sql_get_minute_data)
                with conn.cursor() as cursor :
                    #print(sql_get_minute_data)
                    cursor.execute(sql_get_minute_data)
                    results = cursor.fetchall()
                    #cursor.close()
                    for row in results:
                        acquired_time = row[0]
                        acquired_value = row[1]
                        if not -9999.01 < acquired_value < -9998.99 :
                            accumulated_min_value += acquired_value
                            accumulated_min_samples += 1

                accumulated_min_mean = 0.0
                if accumulated_min_samples > 0 :
                    accumulated_min_mean = accumulated_min_value / accumulated_min_samples

                #if channel_index==20 :
                #    print('sql_get_minute_data', sql_get_minute_data, 'accumulated_min_mean', accumulated_min_mean, 'accumulated_min_samples', accumulated_min_samples)

                sql_insert_accumulated_60 = 'INSERT INTO t_accumulated_data (CHANNEL_INDEX,ACCUMULATED_BIN_END_TIME,ACCUMULATED_BIN_SIZE,ACCUMULATED_NO_OF_SAMPLES,ACCUMULATED_VALUE,ACCUMULATED_TEXT,ACCUMULATED_BINARY) VALUES (%s,%s,%s,%s,%s,%s,%s)'
                with conn.cursor() as cursor :
                    try:
                        cursor.execute(sql_insert_accumulated_60, (channel_index,accumulated_bin_end_time,accumulated_bin_size,accumulated_min_samples,accumulated_min_mean,accumulated_text,accumulated_binary))
                    except pymysql.err.IntegrityError as e:
                        runtime.logging.exception(e)
                    #cursor.close()
                accumulated_min_samples = 0
                accumulated_min_value = 0.0

                if current_minute == 1 :

                    #accumulated_bin_end_time = current_timestamp
                    accumulated_bin_size = 3600

                    sql_get_hour_data = 'SELECT ACQUIRED_TIME,ACQUIRED_VALUE FROM t_acquired_data WHERE CHANNEL_INDEX=' + str(channel_index) + ' AND ACQUIRED_TIME<' + str(accumulated_bin_end_time) + ' AND ACQUIRED_TIME>=' + str(accumulated_bin_end_time - 3600) + ' ORDER BY ACQUIRED_TIME DESC'
                    with conn.cursor() as cursor :
                        cursor.execute(sql_get_hour_data)
                        results = cursor.fetchall()
                        #cursor.close()
                        for row in results:
                            acquired_time = row[0]
                            acquired_value = row[1]
                            if not -9999.01 < acquired_value < -9998.99 :
                                accumulated_hour_value += acquired_value
                                accumulated_hour_samples += 1

                    accumulated_hour_mean = 0.0
                    if accumulated_hour_samples > 0 :
                        accumulated_hour_mean = accumulated_hour_value / accumulated_hour_samples

                    #if channel_index==20 :
                    #    print('sql_get_hour_data', sql_get_hour_data, 'accumulated_hour_mean', accumulated_hour_mean, 'accumulated_hour_samples', accumulated_hour_samples)
                        
                    sql_insert_accumulated_3600 = "INSERT INTO t_accumulated_data (CHANNEL_INDEX,ACCUMULATED_BIN_END_TIME,ACCUMULATED_BIN_SIZE,ACCUMULATED_NO_OF_SAMPLES,ACCUMULATED_VALUE,ACCUMULATED_TEXT,ACCUMULATED_BINARY) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                    with conn.cursor() as cursor :
                        try:
                            cursor.execute(sql_insert_accumulated_3600, (channel_index,accumulated_bin_end_time,accumulated_bin_size,accumulated_hour_samples,accumulated_hour_mean,accumulated_text,accumulated_binary))
                        except pymysql.err.IntegrityError as e:
                            runtime.logging.exception(e)
                        #cursor.close()
                    accumulated_hour_samples = 0
                    accumulated_hour_value = 0.0

                    #sql_get_oldest_time = 'SELECT MIN(ACQUIRED_TIME) FROM T_ACQUIRED_DATA WHERE CHANNEL_INDEX=' + str(channel_index)
                    #oldest_time = 0
                    #with conn.cursor() as cursor :
                    #    cursor.execute(sql_get_oldest_time)
                    #    results = cursor.fetchall()
                    #    for row in results:
                    #        oldest_time = row[0]
                    #runtime.logging.debug(str(oldest_time) + ' ' + str(current_timestamp) + ' ' + str(int(env['TRUNCATE_INTERVAL'] * 24 * 3600)))
                    #if int(oldest_time) < current_timestamp - int(env['TRUNCATE_INTERVAL'] * 24 * 3600) :
                    #    sql_truncate =  'TRUNCATE TABLE T_ACQUIRED_DATA'
                    #    with conn.cursor() as cursor :
                    #        cursor.execute(sql_truncate)
                    #
                    #sql_analyze_table = 'ANALYZE TABLE T_ACQUIRED_DATA'
                    #with conn.cursor() as cursor :
                    #    cursor.execute(sql_analyze_table)

            finally:

                conn.close()

        previous_minute = current_minute
        previous_timestamp = current_timestamp