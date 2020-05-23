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



class Task:


    def __init__(self):

        self.env = self.get_env()

        
    def get_env(self): 

        config = md.Configure(filepath = self.config_filepath, filename = self.config_filename)
        env = config.get()

        return env


        
class StoreSQL(Task):


    def __init__(self):

        Task.__init__(self)
        
        self.delete_sql = "DELETE FROM t_acquired_data WHERE ACQUIRED_TIME=%s AND CHANNEL_INDEX=%s"
        self.insert_sql = "INSERT INTO t_acquired_data (ACQUIRED_TIME,ACQUIRED_MICROSECS,CHANNEL_INDEX,ACQUIRED_VALUE,ACQUIRED_SUBSAMPLES,ACQUIRED_BASE64) VALUES (%s,%s,%s,%s,%s,%s)"


    def connect_db(self):

        try:
            self.conn = pymysql.connect(host = self.env['STORE_DATABASE_HOST'], user = self.env['STORE_DATABASE_USER'], passwd = self.env['STORE_DATABASE_PASSWD'], db = self.env['STORE_DATABASE_DB'], autocommit = True)
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



class LoadFile(StoreSQL):


    def __init__(self):

        StoreSQL.__init__(self)


    def get_filenames(self, channel = None):
      
        files = tf.get_all_files(self.env['STORE_PATH'], self.file_extensions, channel)

        return files



class DataFile(LoadFile):


    def __init__(self):

        LoadFile.__init__(self)


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


    def __init__(self, channels = None, scale_functions = None, config_filepath = None, config_filename = None):
    
    
        self.config_filepath = config_filepath
        self.config_filename = config_filename
        self.channels = channels
        self.file_extensions = ['jpg', 'png']

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


            
class NumpyFile(DataFile):


    def __init__(self, channels = None, scale_functions = None, config_filepath = None, config_filename = None):

        self.config_filepath = config_filepath
        self.config_filename = config_filename
        self.channels = channels
        self.file_extensions = ['npy']
        
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


    def __init__(self, channels = None, scale_functions = None, config_filepath = None, config_filename = None):

        self.config_filepath = config_filepath
        self.config_filename = config_filename
        self.channels = channels
        self.file_extensions = ['csv', 'txt']
        
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
