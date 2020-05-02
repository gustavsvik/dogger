#

import time
import pymysql
import numpy
import os
import math
import metadata
import timefiles
import runtime



class Host:


    def __init__(self):

        self.env = self.get_env()

        
    def get_env(self): 

        config = metadata.Configure(filepath = self.config_filepath, filename = self.config_filename)
        env = config.get()

        return env


        
class Sql(Host):


    def __init__(self):

        Host.__init__(self)
        
        self.delete_sql = "DELETE FROM t_acquired_data WHERE ACQUIRED_TIME=%s AND CHANNEL_INDEX=%s"
        self.insert_sql = "INSERT INTO t_acquired_data (ACQUIRED_TIME,ACQUIRED_MICROSECS,CHANNEL_INDEX,ACQUIRED_VALUE,ACQUIRED_SUBSAMPLES,ACQUIRED_BASE64) VALUES (%s,%s,%s,%s,%s,%s)"


    def connect_db(self):

        try:
            self.conn = pymysql.connect(host = self.env['STORE_DATABASE_HOST'], user = self.env['STORE_DATABASE_USER'], passwd = self.env['STORE_DATABASE_PASSWD'], db = self.env['STORE_DATABASE_DB'], autocommit = True)
        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            runtime.logging.exception(e)


    def commit_transaction(self):
        
        try:
            self.conn.commit()
        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            runtime.logging.exception(e)


    def close_db_connection(self):
        
        try:
            self.conn.close()
        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            runtime.logging.exception(e)



class File(Sql):


    def __init__(self):

        Sql.__init__(self)


    def get_filenames(self, channel = None):
      
        files = timefiles.get_all_files(self.env['STORE_PATH'], self.file_extensions, channel)

        return files


    def get_file_timestamp(self, current_file = None):

        position = current_file.find("_")
        acquired_time_string = current_file[position+1:position+1+10]
        acquired_time = int(acquired_time_string)

        return acquired_time


    def get_file_channel(self, current_file = None):
        runtime.logging.debug(current_file)
        position_after = current_file.find("_")
        string_before_timestamp = current_file[0:position_after]
        position_before = current_file.rfind("/")
        channel_string = current_file[position_before+1:position_after]
        channel = int(channel_string)

        return channel



class DataFile(File):


    def __init__(self):

        #self.channels = channels

        File.__init__(self)


    def retrieve_file_data(self, current_file = None):

        current_channel = self.get_file_channel(current_file)
        acquired_time = self.get_file_timestamp(current_file)
        acquired_time_string = repr(acquired_time)

        acquired_value = -9999.0
        acquired_microsecs = 9999
        acquired_subsamples = ''

        try:
            acquired_values = self.load_file(current_file)
            if len(acquired_values) > 1:
                acquired_subsamples = numpy.array2string(acquired_values[2:], separator=' ', max_line_width=numpy.inf, formatter={'float': lambda x: format(x, '6.5E')})
            acquired_value = acquired_values[0]
            acquired_microsecs = acquired_values[1]
        except (OSError, ValueError) as e:
            runtime.logging.exception(e)
            try:
                os.remove(current_file)
            except (PermissionError, FileNotFoundError) as e:
                runtime.logging.exception(e)

        acquired_base64 = b''

        self.current_channel = current_channel
        self.acquired_time = acquired_time
        self.acquired_microsecs = acquired_microsecs
        self.acquired_value = acquired_value
        self.acquired_subsamples = acquired_subsamples
        self.acquired_base64 = acquired_base64
        self.current_file = current_file


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
                    runtime.logging.exception(e)

            if not math.isnan(float(self.acquired_value)):
                with self.conn.cursor() as cursor :
                    try:
                        runtime.logging.debug(acquired_time_string + channel_string + acquired_value_string + str(self.acquired_subsamples) + str(self.acquired_base64))
                        cursor.execute(self.insert_sql, (acquired_time_string, acquired_microsecs_string, channel_string, acquired_value_string, self.acquired_subsamples[1:-1], self.acquired_base64))
                    except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                        runtime.logging.exception(e)
                    insert_result = cursor.rowcount

        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            runtime.logging.exception(e)
     
        try:
            if insert_result > -1:
                os.remove(self.current_file)
        except (PermissionError, FileNotFoundError) as e:
            runtime.logging.exception(e)

        return insert_result


    def run(self):

        while True:

            time.sleep(0.5)

            for channel_index in self.channels:

                insert_failure = False

                files = self.get_filenames(channel = channel_index)        
                runtime.logging.debug('channel_index' + str(channel_index) + 'len(files)' + str(len(files)))

                if len(files) > 2 :
                        
                    self.connect_db()
                        
                    for current_file in files:

                        if len(files) > 2:

                            self.retrieve_file_data(current_file)
                            store_result = self.store_file_data()
                            if store_result <= -1: insert_failure = True

                    self.close_db_connection()



class ImageFile(File):


    def __init__(self):

        File.__init__(self)



class NumpyFile(DataFile):


    def __init__(self, channels = None, scale_functions = None, config_filepath = None, config_filename = None):

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        DataFile.__init__(self)

        self.channels = channels
        self.file_extensions = ['npy']
        

    def load_file(self, current_file = None):

        return numpy.load(current_file)
