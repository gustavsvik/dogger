#

import os
import glob
import datetime
import time
import pymysql

import gateway.runtime as rt
import gateway.metadata as md
import gateway.task as ta



def get_all_files(path, extensions, channel) :

    files = []

    for extension in extensions :
        file_pattern = str(channel) + '_*.' + extension
        pattern = path + file_pattern
        files += sorted(glob.glob(pattern))

    return files


def delete_multiple(files) :

    for current_file in files:
        try :
            os.remove(current_file)
        except (PermissionError, FileNotFoundError) as e:
            print(e)


def get_timestamped_range(files, lower_timestamp, higher_timestamp) :

    selected_files = []
    for current_file in files :
        start = current_file.rfind('_')
        end = current_file.rfind('.')
        current_timestamp_string = current_file[start+1:end]
        current_timestamp = int(current_timestamp_string)
        if current_timestamp <= higher_timestamp and current_timestamp >= lower_timestamp :
            selected_files.append(current_file)

    return selected_files


def get_file_timestamp(current_file = None):

    before_start_position = current_file.find("_")
    after_end_position = current_file.find(".")
    acquired_time_string = current_file[before_start_position+1:after_end_position]
    acquired_time = int(acquired_time_string)

    return acquired_time


def get_file_local_datetime(current_file = None, datetime_pattern = '%Y%m%d%H%M%S'):

    before_start_position = current_file.find("_")
    after_end_position = current_file.find(".")
    acquired_local_datetime_string = current_file[before_start_position+1:after_end_position]
    local_datetime = datetime.datetime.strptime(acquired_local_datetime_string, datetime_pattern)
    acquired_time = int(time.mktime(local_datetime.timetuple()))

    return acquired_time


def get_file_channel(current_file = None):

    position_after = current_file.find("_")
    string_before_timestamp = current_file[0:position_after]
    position_before = current_file.rfind("/")
    channel_string = current_file[position_before+1:position_after]
    channel = int(channel_string)

    return channel



class SQL:


    def __init__(self, gateway_database_connection = None, config_filepath = None, config_filename = None):

        self.gateway_database_connection = gateway_database_connection

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        self.env = self.get_env()
        if self.gateway_database_connection is None: self.gateway_database_connection = self.env['GATEWAY_DATABASE_CONNECTION']

        self.conn = None


    def get_env(self) :

        config = md.Configure(filepath = self.config_filepath, filename = self.config_filename)
        env = config.get()

        return env


    def connect_db(self) :

        try:
            conn_data = self.gateway_database_connection
            self.conn = pymysql.connect(host = conn_data['host'], user = conn_data['user'], passwd = conn_data['passwd'], db = conn_data['db'], autocommit = True)
        except (pymysql.err.OperationalError, pymysql.err.Error) as e :
            rt.logging.exception(e)

        return self.conn


    def commit_transaction(self) :

        try:
            self.conn.commit()
        except (pymysql.err.OperationalError, pymysql.err.Error) as e :
            rt.logging.exception(e)


    def close_db_connection(self) :

        try:
            if self.conn is not None :
                self.conn.close()
        except (pymysql.err.OperationalError, pymysql.err.Error, NameError) as e :
            rt.logging.exception(e)



class Accumulate(ta.ProcessDataTask) :


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        ta.ProcessDataTask.__init__(self)

        self.sql = SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)

        self.previous_minute = -1
        self.previous_timestamp = int(time.mktime(time.localtime()))


    def run(self) :

        time.sleep(self.start_delay)

        while (True) :

            time.sleep(0.1)

            current_localtime = time.localtime()
            current_hour =  current_localtime.tm_hour
            current_minute = current_localtime.tm_min
            current_second = current_localtime.tm_sec
            current_timestamp = int(time.mktime(time.localtime()))

            if current_timestamp - self.previous_timestamp > 60 :
                current_timestamp = self.previous_timestamp + 60
                current_minute = self.previous_minute + 1
            
            if current_second == 0 and current_minute != self.previous_minute :

                for channel_index in self.channels: 

                    accumulated_min_samples = 0
                    accumulated_min_value = 0.0
                    accumulated_hour_samples = 0
                    accumulated_hour_value = 0.0
                    accumulated_text = ''
                    accumulated_binary = b''
                    
                    try :

                        self.sql.connect_db()

                        accumulated_bin_size = 60
                        accumulated_bin_end_time = current_timestamp - accumulated_bin_size

                        sql_get_minute_data = "SELECT ACQUIRED_TIME,ACQUIRED_VALUE FROM t_acquired_data WHERE CHANNEL_INDEX=%s AND ACQUIRED_TIME<%s AND ACQUIRED_TIME>=%s ORDER BY ACQUIRED_TIME DESC"

                        with self.sql.conn.cursor() as cursor :
                            cursor.execute(sql_get_minute_data, [channel_index, accumulated_bin_end_time, accumulated_bin_end_time - 60] )
                            results = cursor.fetchall()
                            for row in results:
                                acquired_time = row[0]
                                acquired_value = row[1]
                                if not -9999.01 < acquired_value < -9998.99 :
                                    accumulated_min_value += acquired_value
                                    accumulated_min_samples += 1

                        accumulated_min_mean = 0.0
                        if accumulated_min_samples > 0 :
                            accumulated_min_mean = accumulated_min_value / accumulated_min_samples

                        sql_insert_accumulated_60 = "INSERT INTO t_accumulated_data (CHANNEL_INDEX,ACCUMULATED_BIN_END_TIME,ACCUMULATED_BIN_SIZE,ACCUMULATED_NO_OF_SAMPLES,ACCUMULATED_VALUE,ACCUMULATED_TEXT,ACCUMULATED_BINARY) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                        with self.sql.conn.cursor() as cursor :
                            try:
                                cursor.execute(sql_insert_accumulated_60, [channel_index, accumulated_bin_end_time, accumulated_bin_size, accumulated_min_samples, accumulated_min_mean, accumulated_text, accumulated_binary] )
                            except pymysql.err.IntegrityError as e:
                                rt.logging.exception(e)

                        accumulated_min_samples = 0
                        accumulated_min_value = 0.0

                        if current_minute == 1 :

                            accumulated_bin_size = 3600

                            sql_get_hour_data = 'SELECT ACQUIRED_TIME,ACQUIRED_VALUE FROM t_acquired_data WHERE CHANNEL_INDEX=%s AND ACQUIRED_TIME<%s AND ACQUIRED_TIME>=%s ORDER BY ACQUIRED_TIME DESC'
                            with self.sql.conn.cursor() as cursor :
                                cursor.execute(sql_get_hour_data, [channel_index, accumulated_bin_end_time, accumulated_bin_end_time - 3600] )
                                results = cursor.fetchall()
                                for row in results:
                                    acquired_time = row[0]
                                    acquired_value = row[1]
                                    if not -9999.01 < acquired_value < -9998.99 :
                                        accumulated_hour_value += acquired_value
                                        accumulated_hour_samples += 1

                            accumulated_hour_mean = 0.0
                            if accumulated_hour_samples > 0 :
                                accumulated_hour_mean = accumulated_hour_value / accumulated_hour_samples
                                
                            sql_insert_accumulated_3600 = "INSERT INTO t_accumulated_data (CHANNEL_INDEX,ACCUMULATED_BIN_END_TIME,ACCUMULATED_BIN_SIZE,ACCUMULATED_NO_OF_SAMPLES,ACCUMULATED_VALUE,ACCUMULATED_TEXT,ACCUMULATED_BINARY) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                            with self.sql.conn.cursor() as cursor :
                                try:
                                    cursor.execute(sql_insert_accumulated_3600, [channel_index, accumulated_bin_end_time,accumulated_bin_size, accumulated_hour_samples, accumulated_hour_mean, accumulated_text, accumulated_binary] )
                                except pymysql.err.IntegrityError as e:
                                    rt.logging.exception(e)

                            accumulated_hour_samples = 0
                            accumulated_hour_value = 0.0

                    finally:
                    
                        self.sql.close_db_connection()

                        
                self.previous_minute = current_minute
                self.previous_timestamp = current_timestamp
