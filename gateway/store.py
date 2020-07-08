#

import time
import pymysql
import numpy
import os
import math
import base64

import gateway.metadata as md
import gateway.timefiles as tf
import gateway.runtime as rt

import gateway.task as t



class SQL(t.StoreUplink):


    def __init__(self):

        t.StoreUplink.__init__(self)


    def connect_db(self):

        try:
            conn_data = self.gateway_database_connection
            self.conn = pymysql.connect(host = conn_data['host'], user = conn_data['user'], passwd = conn_data['passwd'], db = conn_data['db'], autocommit = True)
        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            rt.logging.exception(e)


    def commit_transaction(self):

        try:
            self.conn.commit()
        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            rt.logging.exception(e)


    def close_db_connection(self):

        try:
            self.conn.close()
        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            rt.logging.exception(e)



class Accumulate(SQL) :


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SQL.__init__(self)

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

                        self.connect_db()

                        accumulated_bin_size = 60
                        accumulated_bin_end_time = current_timestamp - accumulated_bin_size

                        sql_get_minute_data = 'SELECT ACQUIRED_TIME,ACQUIRED_VALUE FROM t_acquired_data WHERE CHANNEL_INDEX=' + str(channel_index) + ' AND ACQUIRED_TIME<' + str(accumulated_bin_end_time) + ' AND ACQUIRED_TIME>=' + str(accumulated_bin_end_time - 60) + ' ORDER BY ACQUIRED_TIME DESC'

                        with self.conn.cursor() as cursor :
                            cursor.execute(sql_get_minute_data)
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

                        sql_insert_accumulated_60 = 'INSERT INTO t_accumulated_data (CHANNEL_INDEX,ACCUMULATED_BIN_END_TIME,ACCUMULATED_BIN_SIZE,ACCUMULATED_NO_OF_SAMPLES,ACCUMULATED_VALUE,ACCUMULATED_TEXT,ACCUMULATED_BINARY) VALUES (%s,%s,%s,%s,%s,%s,%s)'
                        with self.conn.cursor() as cursor :
                            try:
                                cursor.execute(sql_insert_accumulated_60, (channel_index,accumulated_bin_end_time,accumulated_bin_size,accumulated_min_samples,accumulated_min_mean,accumulated_text,accumulated_binary))
                            except pymysql.err.IntegrityError as e:
                                rt.logging.exception(e)

                        accumulated_min_samples = 0
                        accumulated_min_value = 0.0

                        if current_minute == 1 :

                            accumulated_bin_size = 3600

                            sql_get_hour_data = 'SELECT ACQUIRED_TIME,ACQUIRED_VALUE FROM t_acquired_data WHERE CHANNEL_INDEX=' + str(channel_index) + ' AND ACQUIRED_TIME<' + str(accumulated_bin_end_time) + ' AND ACQUIRED_TIME>=' + str(accumulated_bin_end_time - 3600) + ' ORDER BY ACQUIRED_TIME DESC'
                            with self.conn.cursor() as cursor :
                                cursor.execute(sql_get_hour_data)
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
                            with self.conn.cursor() as cursor :
                                try:
                                    cursor.execute(sql_insert_accumulated_3600, (channel_index,accumulated_bin_end_time,accumulated_bin_size,accumulated_hour_samples,accumulated_hour_mean,accumulated_text,accumulated_binary))
                                except pymysql.err.IntegrityError as e:
                                    rt.logging.exception(e)

                            accumulated_hour_samples = 0
                            accumulated_hour_value = 0.0

                    finally:
                    
                        try: 
                            self.conn.close()
                        except NameError:
                            pass

                        
                self.previous_minute = current_minute
                self.previous_timestamp = current_timestamp



class LoadFile(SQL):


    def __init__(self):

        SQL.__init__(self)


    def get_filenames(self, channel = None):
    
        files = tf.get_all_files(path = self.file_path, extensions = self.file_extensions, channel = channel)

        return files



class DataFile(LoadFile):


    def __init__(self):

        LoadFile.__init__(self)

        self.delete_sql = "DELETE FROM t_acquired_data WHERE ACQUIRED_TIME=%s AND CHANNEL_INDEX=%s"
        self.insert_sql = "INSERT INTO t_acquired_data (ACQUIRED_TIME,ACQUIRED_MICROSECS,CHANNEL_INDEX,ACQUIRED_VALUE,ACQUIRED_SUBSAMPLES,ACQUIRED_BASE64) VALUES (%s,%s,%s,%s,%s,%s)"


    def retrieve_file_data(self, current_file = None):

        current_channel = tf.get_file_channel(current_file)
        acquired_time = tf.get_file_timestamp(current_file)
        acquired_time_string = repr(acquired_time)

        self.current_channel = current_channel
        self.acquired_time = acquired_time
        self.current_file = current_file

        self.acquired_microsecs, self.acquired_value, self.acquired_subsamples, self.acquired_base64 = self.load_file(current_file)


    def store_file_data(self):

        channel_string = repr(self.current_channel)
        acquired_time_string = repr(self.acquired_time)
        acquired_microsecs_string = repr(self.acquired_microsecs)
        acquired_value_string = repr(self.acquired_value)

        insert_result = -1

        try:

            with self.conn.cursor() as cursor :

                try:
                    cursor.execute(self.delete_sql, (acquired_time_string, channel_string) )
                except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                    rt.logging.exception(e)

            if not math.isnan(float(self.acquired_value)):
                with self.conn.cursor() as cursor :
                    try:
                        rt.logging.debug(acquired_time_string + channel_string + acquired_value_string + str(self.acquired_subsamples) + str(self.acquired_base64))
                        cursor.execute(self.insert_sql, (acquired_time_string, acquired_microsecs_string, channel_string, acquired_value_string, self.acquired_subsamples[1:-1], self.acquired_base64))
                    except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                        rt.logging.exception(e)
                    insert_result = cursor.rowcount

        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            rt.logging.exception(e)

        try:
            if insert_result > -1:
                os.remove(self.current_file)
        except (PermissionError, FileNotFoundError) as e:
            rt.logging.exception(e)

        return insert_result


    def run(self):

        while True:

            time.sleep(0.5)

            for channel_index in self.channels:

                insert_failure = False

                files = self.get_filenames(channel = channel_index)
                rt.logging.debug('channel_index' + str(channel_index) + 'len(files)' + str(len(files)))

                if len(files) > 2 :

                    self.connect_db()

                    for current_file in files:

                        if len(files) > 2:

                            self.retrieve_file_data(current_file)
                            store_result = self.store_file_data()
                            if store_result <= -1: insert_failure = True

                    self.close_db_connection()



class ImageFile(DataFile):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, file_path = None, file_extensions = ['jpg', 'png'], config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.file_path = file_path
        self.file_extensions = file_extensions

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        DataFile.__init__(self)


    def load_file(self, current_file = None):

        acquired_microsecs = 9999
        acquired_value = -9999.0
        acquired_subsamples = ''
        acquired_base64 = b''

        try :
            with open(current_file, "rb") as image_file :
                acquired_base64 = base64.b64encode(image_file.read())
        except OSError as e :
            rt.logging.exception(e)
            try:
                os.remove(current_file)
            except (PermissionError, FileNotFoundError, OSError) as e:
                rt.logging.exception(e)

        return acquired_microsecs, acquired_value, acquired_subsamples, acquired_base64



class ScreenshotFile(ImageFile):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, file_path = None, file_extensions = ['jpeg'], config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.file_path = file_path
        self.file_extensions = file_extensions

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        ImageFile.__init__(self, channels, start_delay, gateway_database_connection, file_path, file_extensions, config_filepath, config_filename)


    def retrieve_file_data(self, current_file = None):

        current_channel = tf.get_file_channel(current_file)
        acquired_time = tf.get_file_local_datetime(current_file, datetime_pattern = '%Y%m%d%H%M%S')
        acquired_time_string = repr(acquired_time)

        self.current_channel = current_channel
        self.acquired_time = acquired_time
        self.current_file = current_file

        self.acquired_microsecs, self.acquired_value, self.acquired_subsamples, self.acquired_base64 = self.load_file(current_file)



class NumpyFile(DataFile):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, file_path = None, file_extensions = ['npy'], config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.file_path = file_path
        self.file_extensions = file_extensions

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        DataFile.__init__(self)


    def load_file(self, current_file = None):

        acquired_microsecs = 9999
        acquired_value = -9999.0
        acquired_subsamples = ''
        acquired_base64 = b''

        try:
            acquired_values = numpy.load(current_file)
            if len(acquired_values) > 1:
                # acquired_subsamples = base64.b64encode( (acquired_values[2:]).astype('float32', casting = 'same_kind') )
                acquired_subsamples = numpy.array2string(acquired_values[2:], separator=' ', max_line_width = numpy.inf, formatter = {'float': lambda x: format(x, '6.5E')})
            acquired_value = acquired_values[0]
            acquired_microsecs = acquired_values[1]
        except (OSError, ValueError, IndexError) as e:
            rt.logging.exception(e)
            try:
                os.remove(current_file)
            except (PermissionError, FileNotFoundError, OSError) as e:
                rt.logging.exception(e)

        return acquired_microsecs, acquired_value, acquired_subsamples, acquired_base64


class TextFile(DataFile):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, file_path = None, file_extensions = ['csv', 'txt'], config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.file_path = file_path
        self.file_extensions = file_extensions

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        DataFile.__init__(self)


    def load_file(self, current_file = None):

        acquired_microsecs = 9999
        acquired_value = -9999.0
        acquired_subsamples = ''
        acquired_base64 = b''

        try:
            acquired_values = numpy.genfromtxt(current_file, delimiter = ',')
            if len(acquired_values) > 1:
                acquired_subsamples = numpy.array2string(acquired_values[2:], separator=' ', max_line_width = numpy.inf, formatter = {'float': lambda x: format(x, '6.5E')})
            acquired_value = acquired_values[0]
            acquired_microsecs = acquired_values[1]
        except (OSError, ValueError, IndexError) as e:
            rt.logging.exception(e)
            try:
                os.remove(current_file)
            except (PermissionError, FileNotFoundError, OSError) as e:
                rt.logging.exception(e)

        return acquired_microsecs, acquired_value, acquired_subsamples, acquired_base64
