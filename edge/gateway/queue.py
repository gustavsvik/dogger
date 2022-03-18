#

import time
import pymysql
import numpy
import os
import math
import base64

try : import msgpack_numpy
except ImportError: pass

import gateway.runtime as rt
import gateway.persist as ps



class FileToSQL(ps.LoadFile):


    def __init__(self):

        ps.LoadFile.__init__(self)
        self.sql = ps.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)

        if self.files_to_keep is None : self.files_to_keep = 2


    def store_file_data(self):

        # channel_string = repr(self.current_channel)
        # acquired_time_string = repr(self.acquired_time)
        # acquired_microsecs_string = repr(self.acquired_microsecs)
        # acquired_value_string = repr(self.acquired_value)

        # insert_result = -1

        # try:

            # with self.sql.conn.cursor() as cursor :

                # try:
                    # delete_sql = "DELETE FROM t_acquired_data WHERE ACQUIRED_TIME=%s AND CHANNEL_INDEX=%s"
                    # cursor.execute(delete_sql, (acquired_time_string, channel_string) )
                # except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                    # rt.logging.exception(e)

            # if not math.isnan(float(self.acquired_value)):
                # with self.sql.conn.cursor() as cursor :
                    # try:
                        # rt.logging.debug(acquired_time_string + channel_string + acquired_value_string + str(self.acquired_text) + str(self.acquired_bytes))
                        # insert_sql = "INSERT INTO t_acquired_data (ACQUIRED_TIME,ACQUIRED_MICROSECS,CHANNEL_INDEX,ACQUIRED_VALUE,ACQUIRED_TEXT,ACQUIRED_BYTES,STATUS) VALUES (%s,%s,%s,%s,%s,%s,0)"
                        # cursor.execute(insert_sql, (acquired_time_string, acquired_microsecs_string, channel_string, acquired_value_string, self.acquired_text, self.acquired_bytes))
                    # except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                        # rt.logging.exception(e)
                    # insert_result = cursor.rowcount

        # except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            # rt.logging.exception(e)

        insert_result = self.sql.store_acquired_record(self.current_channel, self.acquired_time, self.acquired_microsecs, self.acquired_value, self.acquired_text, self.acquired_bytes)

        try:
            if insert_result > -1:
                os.remove(self.current_file)
        except (PermissionError, FileNotFoundError) as e:
            rt.logging.exception(e)

        return insert_result


    def run(self):

        time.sleep(self.start_delay)

        while True:

            time.sleep(0.5)

            for channel_index in self.channels :

                insert_failure = False

                files = self.get_filenames(channel = channel_index)
                rt.logging.debug('channel_index' + str(channel_index) + 'len(files)' + str(len(files)))

                if len(files) > self.files_to_keep :

                    if self.sql.connect_db() :

                        for current_file in files :

                            if len(files) > self.files_to_keep :

                                self.retrieve_file_data(current_file)
                                store_result = self.store_file_data()
                                if store_result <= -1: insert_failure = True

                        self.sql.close_db_connection()



class ImageFile(FileToSQL):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, file_path = None, file_extensions = ['jpg', 'png'], files_to_keep = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.file_path = file_path
        self.file_extensions = file_extensions
        self.files_to_keep = files_to_keep

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        FileToSQL.__init__(self)


    def load_file(self, current_file = None):

        acquired_microsecs = 9999
        acquired_value = -9999.0
        acquired_text = ''
        acquired_bytes = b''

        try :
            with open(current_file, "rb") as image_file :
                acquired_bytes = base64.b64encode(image_file.read())
        except OSError as e :
            rt.logging.exception(e)
            try:
                os.remove(current_file)
            except (PermissionError, FileNotFoundError, OSError) as e:
                rt.logging.exception(e)

        return acquired_microsecs, acquired_value, acquired_text, acquired_bytes



class ScreenshotFile(ImageFile):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, file_path = None, file_extensions = ['jpeg'], files_to_keep = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.file_path = file_path
        self.file_extensions = file_extensions
        self.files_to_keep = files_to_keep

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        ImageFile.__init__(self, channels, start_delay, gateway_database_connection, file_path, file_extensions, files_to_keep, config_filepath, config_filename)


    def retrieve_file_data(self, current_file = None):

        current_channel = ps.get_file_channel(current_file)
        acquired_time = ps.get_file_local_datetime(current_file, datetime_pattern = '%Y%m%d%H%M%S')
        acquired_time_string = repr(acquired_time)

        self.current_channel = current_channel
        self.acquired_time = acquired_time
        self.current_file = current_file

        self.acquired_microsecs, self.acquired_value, self.acquired_text, self.acquired_bytes = self.load_file(current_file)



class NumpyFile(FileToSQL):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, file_path = None, file_extensions = ['npy'], files_to_keep = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.file_path = file_path
        self.file_extensions = file_extensions
        self.files_to_keep = files_to_keep

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        FileToSQL.__init__(self)


    def load_file(self, current_file = None):

        acquired_microsecs = 9999
        acquired_value = -9999.0
        acquired_text = ''
        acquired_bytes = b''

        try:
            acquired_values = numpy.load(current_file)
            if len(acquired_values) > 1:
                # acquired_bytes = base64.b64encode( (acquired_values[2:]).astype('float32', casting = 'same_kind') )
                # acquired_text = numpy.array2string(acquired_values[2:], separator=' ', max_line_width = numpy.inf, formatter = {'float': lambda x: format(x, '6.5E')})
                subsample_string = numpy.array2string(acquired_values[2:], separator=' ', max_line_width = numpy.inf, formatter = {'float': lambda x: format(x, '6.5E')})
                acquired_bytes = subsample_string[1:-1]
            acquired_value = acquired_values[0]
            acquired_microsecs = acquired_values[1]
        except (OSError, ValueError, IndexError) as e:
            rt.logging.exception(e)
            try:
                os.remove(current_file)
            except (PermissionError, FileNotFoundError, OSError) as e:
                rt.logging.exception(e)

        return acquired_microsecs, acquired_value, acquired_text, acquired_bytes



class NumpyFileMsgpack(FileToSQL):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, file_path = None, file_extensions = ['npy'], files_to_keep = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.file_path = file_path
        self.file_extensions = file_extensions
        self.files_to_keep = files_to_keep

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        FileToSQL.__init__(self)


    def load_file(self, current_file = None):

        acquired_microsecs = 9999
        acquired_value = -9999.0
        acquired_text = ''
        acquired_bytes = b''

        try:
            acquired_values = numpy.load(current_file)
            if len(acquired_values) > 1:
                # acquired_bytes = base64.b64encode( (acquired_values[2:]).astype('float32', casting = 'same_kind') )
                # acquired_text = numpy.array2string(acquired_values[2:], separator=' ', max_line_width = numpy.inf, formatter = {'float': lambda x: format(x, '6.5E')})
                subsample_string = numpy.array2string(acquired_values[2:], separator=' ', max_line_width = numpy.inf, formatter = {'float': lambda x: format(x, '6.5E')})
                packed = msgpack_numpy.packb(acquired_values[2:], default = msgpack_numpy.encode)
                rt.logging.debug("packed", packed, len(packed), "len(packed)")
                packed_ascii = base64.b64encode(packed)
                rt.logging.debug("packed_ascii", packed_ascii, len(packed_ascii), "len(packed_ascii)")
                acquired_bytes = packed_ascii
            #acquired_value = acquired_values[0]
            acquired_microsecs = acquired_values[1]
        except (OSError, ValueError, IndexError) as e:
            rt.logging.exception(e)
            try:
                os.remove(current_file)
            except (PermissionError, FileNotFoundError, OSError) as e:
                rt.logging.exception(e)

        return acquired_microsecs, acquired_value, acquired_text, acquired_bytes



class TextFile(FileToSQL):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, file_path = None, file_extensions = ['csv', 'txt'], files_to_keep = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.file_path = file_path
        self.file_extensions = file_extensions
        self.files_to_keep = files_to_keep

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        FileToSQL.__init__(self)


    def load_file(self, current_file = None):

        acquired_microsecs = 9999
        acquired_value = -9999.0
        acquired_text = ''
        acquired_bytes = b''

        try:
            acquired_values = numpy.genfromtxt(current_file, delimiter = ',')
            if len(acquired_values) > 1:
                acquired_text = numpy.array2string(acquired_values[2:], separator=' ', max_line_width = numpy.inf, formatter = {'float': lambda x: format(x, '6.5E')})
            acquired_value = acquired_values[0]
            acquired_microsecs = acquired_values[1]
        except (OSError, ValueError, IndexError) as e:
            rt.logging.exception(e)
            try:
                os.remove(current_file)
            except (PermissionError, FileNotFoundError, OSError) as e:
                rt.logging.exception(e)

        return acquired_microsecs, acquired_value, acquired_text, acquired_bytes



class TextStringFile(FileToSQL):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, file_path = None, file_extensions = ['csv', 'txt', 'json'], files_to_keep = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.file_path = file_path
        self.file_extensions = file_extensions
        self.files_to_keep = files_to_keep

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        FileToSQL.__init__(self)


    def load_file(self, current_file = None):

        return ps.load_text_string_file(current_file)


class TextJsonFile(FileToSQL):


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, file_path = None, file_extensions = ['csv', 'txt', 'json'], files_to_keep = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection
        self.file_path = file_path
        self.file_extensions = file_extensions
        self.files_to_keep = files_to_keep

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        FileToSQL.__init__(self)


    def load_file(self, current_file = None):

        return ps.load_text_json_file(current_file)
