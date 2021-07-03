#

import os
import glob
import json
import datetime
import time
import pymysql
import math
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
            rt.logging.exception(e)


def get_timestamped_range(files, lower_timestamp, higher_timestamp) :

    selected_files = []
    for current_file in files :
        try :
            start = current_file.rfind('_')
            end = current_file.rfind('.')
            current_timestamp_string = current_file[start+1:end]
            current_timestamp = int(current_timestamp_string)
            if current_timestamp <= higher_timestamp and current_timestamp >= lower_timestamp :
                selected_files.append(current_file)
        except (AttributeError, TypeError, ValueError) as e :
            rt.logging.exception(e)
    return selected_files


def get_file_timestamp(current_file = None) :

    acquired_time = None
    try :
        before_start_position = current_file.find("_")
        after_end_position = current_file.find(".")
        acquired_time_string = current_file[before_start_position+1:after_end_position]
        acquired_time = int(acquired_time_string)
    except (AttributeError, TypeError, ValueError) as e :
        rt.logging.exception(e)
    return acquired_time


def get_file_local_datetime(current_file = None, datetime_pattern = '%Y%m%d%H%M%S') :

    acquired_time = None
    try :
        before_start_position = current_file.find("_")
        after_end_position = current_file.find(".")
        acquired_local_datetime_string = current_file[before_start_position+1:after_end_position]
        local_datetime = datetime.datetime.strptime(acquired_local_datetime_string, datetime_pattern)
        acquired_time = int(time.mktime(local_datetime.timetuple()))
    except (AttributeError, TypeError, ValueError) as e :
        rt.logging.exception(e)
    return acquired_time


def get_file_channel(current_file = None) :

    channel = None
    try :
        position_after = current_file.find("_")
        string_before_timestamp = current_file[0:position_after]
        position_before = current_file.rfind("/")
        channel_string = current_file[position_before+1:position_after]
        channel = int(channel_string)
    except (AttributeError, TypeError, ValueError) as e :
        rt.logging.exception(e)
    return channel


def load_text_json_file(current_file = None):

    acquired_microsecs = 9999
    acquired_value = -9999.0
    acquired_text = ''
    acquired_bytes = b''

    try :
        with open(current_file, "r") as text_file :
            text_string = text_file.read()
            try :
                rt.logging.debug("json.loads(text_string)", json.loads(text_string) )
            except json.decoder.JSONDecodeError as e :
                rt.logging.exception(e)
            acquired_bytes = text_string.encode("utf-8")
    except OSError as e :
        rt.logging.exception(e)
        try:
            os.remove(current_file)
        except (PermissionError, FileNotFoundError, OSError) as e:
            rt.logging.exception(e)

    return acquired_microsecs, acquired_value, acquired_text, acquired_bytes


def load_text_string_file(current_file = None):

    acquired_microsecs = 9999
    acquired_value = -9999.0
    acquired_text = ''
    acquired_bytes = b''

    try :
        with open(current_file, "r") as text_file :
            text_string = text_file.read()
            acquired_bytes = text_string.encode("utf-8") # base64.b64encode(
    except OSError as e :
        rt.logging.exception(e)
        try:
            os.remove(current_file)
        except (PermissionError, FileNotFoundError, OSError) as e:
            rt.logging.exception(e)

    return acquired_microsecs, acquired_value, acquired_text, acquired_bytes



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

        self.acquired_microsecs, self.acquired_value, self.acquired_text, self.acquired_bytes = self.load_file(current_file)



class IngestFile(AcquireControlFile) :


    def __init__(self) :

        self.env = self.get_env()
        if self.archive_file_path is None: self.archive_file_path = self.env['ARCHIVE_FILE_PATH']

        AcquireControlFile.__init__(self)


    def write(self, target_channels = None, data_array = None, selected_tag = None, timestamp_secs = None, timestamp_microsecs = None, sample_rate = None, write_interval = None) :

        write_channels = self.channels
        if target_channels is not None :
            write_channels = target_channels
        rt.logging.debug("write_channels", write_channels)
        rt.logging.debug("data_array", data_array)
        selected_channel_array = [ tag_channels for channel_tag, tag_channels in write_channels.items() if channel_tag == selected_tag ]
        rt.logging.debug("selected_channel_array", selected_channel_array)

        for channel, file_type in selected_channel_array[0].items() :
            rt.logging.debug("channel, file_type", channel, file_type) 
            rt.logging.debug("selected_channel_array[0][channel]", selected_channel_array[0][channel])
            rt.logging.debug("self.file_path", self.file_path)
            if self.file_path is not None and os.path.exists(self.file_path) and channel > 0 :

                if timestamp_secs is None and timestamp_microsecs is None and sample_rate is not None :
                    timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = sample_rate)

                if timestamp_microsecs is None : timestamp_microsecs = 0

                capture_file_timestamp = timestamp_secs 
                store_filename = self.file_path + str(channel) + '_' + str(capture_file_timestamp) + '.' + file_type

                try :

                    channel_array = data_array[0][channel]

                    if file_type == 'npy' :

                        float_avg = tr.downsample(numpy.float64(channel_array), 1)
                        channel_array = numpy.concatenate(([0.0], timestamp_microsecs, channel_array), axis = None)
                        channel_array[0] = float_avg
                        rt.logging.debug("channel_array", channel_array)
                        try :
                            numpy.save(store_filename, channel_array)
                        except PermissionError as e:
                            rt.logging.exception(e)

                    if file_type == 'txt' and channel_array != '' :
                        try :
                            with open(store_filename, 'w') as text_file :
                                rt.logging.debug("data_array", data_array)
                                rt.logging.debug("channel", channel)
                                text_file.write(channel_array)
                        except PermissionError as e:
                            rt.logging.exception(e)

                    if file_type == 'json' and len(channel_array) > 0 :
                        try :
                            with open(store_filename, 'w') as text_file :
                                channel_array_string = json.dumps(channel_array)
                                rt.logging.debug("channel_array_string", channel_array_string)
                                text_file.write(channel_array_string)
                        except PermissionError as e:
                            rt.logging.exception(e)

                except KeyError as e : 

                    rt.logging.exception(e)

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


    def get_stored(self, selected_channels, max_age) :

        channels = []
        timestamps = []
        values = []
        byte_strings = []

        for channel in selected_channels :

            current_timestamp = int(time.time())
            back_timestamp = current_timestamp - max_age

            sql_get_values = "SELECT AD.ACQUIRED_TIME,AD.ACQUIRED_VALUE,AD.ACQUIRED_TEXT,AD.ACQUIRED_BYTES FROM t_acquired_data AD WHERE AD.CHANNEL_INDEX=" + str(channel) + " AND AD.ACQUIRED_TIME BETWEEN " + str(back_timestamp) + " AND " + str(current_timestamp) + " AND AD.STATUS>=0 ORDER BY AD.ACQUIRED_TIME DESC" 
            rt.logging.debug("sql_get_values",sql_get_values)

            with self.conn.cursor() as cursor :
                results = []
                try:
                    cursor.execute(sql_get_values)
                    results = cursor.fetchall()
                except (pymysql.err.IntegrityError, pymysql.err.InternalError, pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e :
                    rt.logging.exception(e)
                rt.logging.debug(results)
                if len(results) > 0 :
                    row = results[0]
                    acquired_time = row[0]
                    acquired_value = row[1]
                    acquired_text = row[2]
                    acquired_bytes = row[3]
                    bytes_string = ''
                    if acquired_bytes is not None :
                        bytes_string = acquired_bytes.decode('utf-8')
                    rt.logging.debug("Channel: ", str(channel), ", Value: ", acquired_value, ", Timestamp: ", acquired_time) #, ", Sub-samples: ", acquired_text, ", bytes: ", acquired_bytes)
                    channels.append(channel)
                    timestamps.append(acquired_time)
                    values.append(acquired_value)
                    byte_strings.append(acquired_bytes)

        return channels, timestamps, values, byte_strings


    def get_requests(self, channel_list, timestamp_list) :

        return_string = None

        if not ( None in [channel_list, timestamp_list] ) and not ( 0 in [len(channel_list), len(timestamp_list)] ) : 

            try:

                self.connect_db()

                return_string = ""

                for channel_index in range(len(channel_list)):

                    requested_timestamps = [int(ts_string) for ts_string in timestamp_list[channel_index][:-1]] 
                    channel_string = channel_list[channel_index][0]

                    if not requested_timestamps :
                        pass
                    else :
                        sql_timestamps = '(' + ','.join([str(ts) for ts in requested_timestamps]) + ')'

                        sql_get_values = "SELECT AD.ACQUIRED_TIME,AD.ACQUIRED_VALUE,AD.ACQUIRED_TEXT,AD.ACQUIRED_BYTES FROM t_acquired_data AD WHERE AD.CHANNEL_INDEX=" + channel_string + " AND AD.ACQUIRED_TIME IN " + sql_timestamps + " AND AD.STATUS>=0"
                        rt.logging.debug("sql_get_values",sql_get_values)

                        return_string += channel_string + ';'
                        with self.conn.cursor() as cursor :
                            results = []
                            try:
                                cursor.execute(sql_get_values)
                                results = cursor.fetchall()
                            except (pymysql.err.IntegrityError, pymysql.err.InternalError, pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
                                rt.logging.exception(e)
                            for row in results:
                                acquired_time = row[0]
                                acquired_value = row[1]
                                acquired_text = row[2]
                                acquired_bytes = row[3]
                                bytes_string = acquired_bytes.decode('utf-8')
                                rt.logging.debug("Channel: ", channel_string, ", Value: ", acquired_value, ", Timestamp: ", acquired_time, ", Sub-samples: ", acquired_text, ", bytes: ", acquired_bytes)
                                if acquired_time in requested_timestamps:
                                    requested_timestamps.remove(acquired_time)
                                return_string += str(acquired_time) + ',' + str(acquired_value) + ',' + str(acquired_text) + ',' + str(bytes_string) + ','
                        return_string += ';'

                        if len(requested_timestamps) > 0:
                            if min(requested_timestamps) < int(time.time()) - 3600:
                                r_clear = self.clear_data_requests(ip)

            except (pymysql.err.OperationalError, pymysql.err.Error) as e:
                rt.logging.exception(e)

            finally:

                self.close_db_connection()
                
        return return_string


    def store_acquired_record(self, channel, acquired_time, acquired_microsecs, acquired_value, acquired_text, acquired_bytes):

        channel_string = repr(channel)
        acquired_time_string = repr(acquired_time)
        acquired_microsecs_string = repr(acquired_microsecs)
        acquired_value_string = repr(acquired_value)

        insert_result = -1

        try:

            with self.conn.cursor() as cursor :

                try:
                    delete_sql = "DELETE FROM t_acquired_data WHERE ACQUIRED_TIME=%s AND CHANNEL_INDEX=%s"
                    cursor.execute(delete_sql, (acquired_time_string, channel_string) )
                except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                    rt.logging.exception(e)

            if not math.isnan(float(acquired_value)):
                with self.conn.cursor() as cursor :
                    try:
                        rt.logging.debug(acquired_time_string + channel_string + acquired_value_string + str(acquired_text) + str(acquired_bytes))
                        insert_sql = "INSERT INTO t_acquired_data (ACQUIRED_TIME,ACQUIRED_MICROSECS,CHANNEL_INDEX,ACQUIRED_VALUE,ACQUIRED_TEXT,ACQUIRED_BYTES,STATUS) VALUES (%s,%s,%s,%s,%s,%s,0)"
                        cursor.execute(insert_sql, (acquired_time_string, acquired_microsecs_string, channel_string, acquired_value_string, acquired_text, acquired_bytes))
                    except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                        rt.logging.exception(e)
                    insert_result = cursor.rowcount

        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            rt.logging.exception(e)

        return insert_result


    def store_new_acquired_list(self, channel_list, times_list, values_list, byte_string_list):
    
        no_of_inserted_rows = 0

        try:
            self.connect_db()
            for channel, times, values, byte_strings in zip(channel_list, times_list, values_list, byte_string_list) :
                rt.logging.debug("channel", channel, "times", times, "values", values, "byte_strings", byte_strings)
                channel= channel[0]
                for timestamp, value, byte_string in zip(times, values, byte_strings) :
                    replaced_byte_string = tr.de_armor_separators(byte_string)
                    rt.logging.debug("replaced_byte_string", replaced_byte_string)
                    check_present_sql = "SELECT AD.UNIQUE_INDEX FROM t_acquired_data AD WHERE AD.ACQUIRED_TIME=" + str(timestamp) + " AND AD.CHANNEL_INDEX=" + str(channel) + " AND AD.STATUS>=0"
                    with self.conn.cursor() as cursor :
                        rows_already_present = []
                        try:
                            cursor.execute(check_present_sql)
                            rows_already_present = cursor.fetchall()
                        except (pymysql.err.IntegrityError, pymysql.err.InternalError, pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e :
                            rt.logging.exception(e)
                        rt.logging.debug(rows_already_present)
                        if len(rows_already_present) == 0 :
                            with self.conn.cursor() as cursor :
                                # TODO: precede by SELECT to avoid INSERT attempts causing primary key violations
                                sql = "INSERT INTO t_acquired_data (ACQUIRED_TIME,CHANNEL_INDEX,ACQUIRED_VALUE,ACQUIRED_BYTES,STATUS) VALUES (" + str(timestamp) + "," + str(channel) + "," + str(value) + ",'" + replaced_byte_string + "',0)"
                                rt.logging.debug("sql", sql)
                                try:
                                    cursor.execute(sql)
                                except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                                    rt.logging.exception(e)
                                no_of_inserted_rows = cursor.rowcount
                                rt.logging.debug("no_of_inserted_rows", no_of_inserted_rows)

        except (pymysql.err.OperationalError, pymysql.err.Error) as e:

            rt.logging.exception(e)

        finally:

            #try:
            self.close_db_connection()
            #except pymysql.err.Error as e:
            #    rt.logging.exception(e)

        return no_of_inserted_rows


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
                    accumulated_bytes = b''
                    
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

                        sql_insert_accumulated_60 = "INSERT INTO t_accumulated_data (CHANNEL_INDEX,ACCUMULATED_BIN_END_TIME,ACCUMULATED_BIN_SIZE,ACCUMULATED_NO_OF_SAMPLES,ACCUMULATED_VALUE,ACCUMULATED_TEXT,ACCUMULATED_BYTES) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                        with self.sql.conn.cursor() as cursor :
                            try:
                                cursor.execute(sql_insert_accumulated_60, [channel_index, accumulated_bin_end_time, accumulated_bin_size, accumulated_min_samples, accumulated_min_mean, accumulated_text, accumulated_bytes] )
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
                                
                            sql_insert_accumulated_3600 = "INSERT INTO t_accumulated_data (CHANNEL_INDEX,ACCUMULATED_BIN_END_TIME,ACCUMULATED_BIN_SIZE,ACCUMULATED_NO_OF_SAMPLES,ACCUMULATED_VALUE,ACCUMULATED_TEXT,ACCUMULATED_BYTES) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                            with self.sql.conn.cursor() as cursor :
                                try:
                                    cursor.execute(sql_insert_accumulated_3600, [channel_index, accumulated_bin_end_time,accumulated_bin_size, accumulated_hour_samples, accumulated_hour_mean, accumulated_text, accumulated_bytes] )
                                except pymysql.err.IntegrityError as e:
                                    rt.logging.exception(e)

                            accumulated_hour_samples = 0
                            accumulated_hour_value = 0.0

                    finally:
                    
                        self.sql.close_db_connection()

                        
                self.previous_minute = current_minute
                self.previous_timestamp = current_timestamp
