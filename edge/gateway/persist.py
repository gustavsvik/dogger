#

import os
import glob
import datetime
import time
import pymysql
import numpy

import gateway.runtime as rt
import gateway.metadata as md
import gateway.task as ta
import gateway.transform as tr



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


def load_text_string_file(current_file = None):

    acquired_microsecs = 9999
    acquired_value = -9999.0
    acquired_subsamples = ''
    acquired_base64 = b''

    try :
        with open(current_file, "r") as text_file :
            text_string = text_file.read()
            acquired_base64 = text_string.encode("utf-8") # base64.b64encode(
    except OSError as e :
        rt.logging.exception(e)
        try:
            os.remove(current_file)
        except (PermissionError, FileNotFoundError, OSError) as e:
            rt.logging.exception(e)

    return acquired_microsecs, acquired_value, acquired_subsamples, acquired_base64



class AcquireControlFile(ta.AcquireControlTask) :


    def __init__(self) :

        self.env = self.get_env()
        if self.file_path is None: self.file_path = self.env['FILE_PATH']

        ta.AcquireControlTask.__init__(self)



class ProcessFile(ta.ProcessDataTask) :


    def __init__(self) :

        self.env = self.get_env()
        if self.file_path is None: self.file_path = self.env['FILE_PATH']

        ta.ProcessDataTask.__init__(self)



class LoadFile(ProcessFile):


    def __init__(self):

        ProcessFile.__init__(self)


    def get_filenames(self, channel = None):
    
        files = get_all_files(path = self.file_path, extensions = self.file_extensions, channel = channel)

        return files


    def retrieve_file_data(self, current_file = None):

        current_channel = get_file_channel(current_file)
        acquired_time = get_file_timestamp(current_file)
        acquired_time_string = repr(acquired_time)

        self.current_channel = current_channel
        self.acquired_time = acquired_time
        self.current_file = current_file

        self.acquired_microsecs, self.acquired_value, self.acquired_subsamples, self.acquired_base64 = self.load_file(current_file)



class IngestFile(AcquireControlFile) :


    def __init__(self) :

        self.env = self.get_env()
        if self.archive_file_path is None: self.archive_file_path = self.env['ARCHIVE_FILE_PATH']

        AcquireControlFile.__init__(self)


    def write(self, data_array = None, selected_tag = None, timestamp_secs = None, timestamp_microsecs = None, sample_rate = None, write_interval = None) :

        rt.logging.debug("data_array", data_array)
        selected_channel_array = [ tag_channels for channel_tag, tag_channels in self.channels.items() if channel_tag == selected_tag ]
        rt.logging.debug("selected_channel_array", selected_channel_array)

        for channel, file_type in selected_channel_array[0].items() :
            rt.logging.debug ("channel, file_type", channel, file_type) 
            rt.logging.debug("selected_channel_array[0][channel]", selected_channel_array[0][channel])
            if self.file_path is not None and os.path.exists(self.file_path) :

                if timestamp_secs is None and timestamp_microsecs is None and sample_rate is not None :
                    timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = sample_rate)

                if timestamp_microsecs is None : timestamp_microsecs = 0

                capture_file_timestamp = timestamp_secs 
                store_filename = self.file_path + str(channel) + '_' + str(capture_file_timestamp) + '.' + file_type

                try :

                    if file_type == 'npy' :
                        float_array = data_array[0][channel]
                        float_avg = tr.downsample(numpy.float64(float_array), 1)
                        float_array = numpy.concatenate(([0.0], timestamp_microsecs, float_array), axis = None)
                        float_array[0] = float_avg
                        print("store_filename", store_filename, "float_array", float_array)
                        try :
                            numpy.save(store_filename, float_array)
                        except PermissionError as e:
                            rt.logging.exception(e)

                    if file_type == 'txt' and data_array[0][channel] != '' :
                        try :
                            with open(store_filename, 'w') as text_file :
                                text_file.write(data_array[0][channel])
                        except PermissionError as e:
                            rt.logging.exception(e)

                except KeyError as e : 

                    print(e)

                if self.file_path is not None and os.path.exists(self.file_path):
                    try:
                        if self.archive_file_path is not None and os.path.exists(self.archive_file_path) :
                            archive_filename = self.archive_file_path + str(channel) + '_' + str(capture_file_timestamp) + '.' + file_type
                            #shutil.copy(store_filename, archive_filename)
                    except (FileNotFoundError, PermissionError) as e:
                        rt.logging.exception(e)

                if write_interval is not None :
                    time.sleep(self.write_interval)



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
