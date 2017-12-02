#

import time
import pymysql
import numpy
import os
import math
import metadata


class Host:


    def __init__(self, channels = None, scale_functions = None):
        
        self.channels = channels
        self.scale_functions = scale_functions



class Sql(Host):


    def __init__(self, channels = None, scale_functions = None):

        Host.__init__(self, channels = None, scale_functions = None)
        
        self.host = 'localhost'
        self.user = 'root'
        self.passwd = 'root'
        self.db = 'test'
        self.autocommit = True

        self.delete_sql = "DELETE FROM T_ACQUIRED_DATA WHERE ACQUIRED_TIME=%s AND CHANNEL_INDEX=%s"
        self.insert_sql = "INSERT INTO T_ACQUIRED_DATA (ACQUIRED_TIME,CHANNEL_INDEX,ACQUIRED_VALUE,ACQUIRED_SUBSAMPLES,ACQUIRED_BASE64) VALUES (%s,%s,%s,%s,%s)"


    def connect_db(self):

        try:
            self.conn = pymysql.connect(host = self.host, user = self.user, passwd = self.passwd, db = self.db, autocommit = self.autocommit)
        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            print(e)


    def commit_transaction(self):
        
        try:
            self.conn.commit()
        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            print(e)


    def close_db_connection(self):
        
        try:
            self.conn.close()
        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            print(e)



class File(Sql):


    def __init__(self, channels = None, scale_functions = None):

        Sql.__init__(self, channels = None, scale_functions = None)

        config = metadata.Configure()
        self.dataFilepath = config.getDataFilePath()


    def get_filenames(self, channel = None):
        
        self.files = []
        
        with os.scandir(self.dataFilepath) as it:
            for entry in it:
                if entry.name.startswith(repr(channel) + '_') and entry.is_file():
                    self.files.append(entry.name)

        return self.files


    def get_file_timestamp(self, current_file = None):

        position = current_file.find("_")
        acquired_time_string = current_file[position+1:position+1+10]
        acquired_time = int(acquired_time_string)

        return acquired_time


    def get_file_channel(self, current_file = None):

        position = current_file.find("_")
        channel_string = current_file[0:position]
        channel = int(channel_string)

        return channel



class DataFile(File):


    def __init__(self, channels = None, scale_functions = None):

        File.__init__(self, channels = None, scale_functions = None)


    def retrieve_file_data(self, current_file = None):

        current_channel = self.get_file_channel(current_file)
        acquired_time = self.get_file_timestamp(current_file)
        acquired_time_string = repr(acquired_time)

        acquired_value = -9999.0
        acquired_subsamples = ''

        try:
            acquired_values = self.load_file(self.dataFilepath + current_file)
            if len(acquired_values) > 1:
                acquired_subsamples = acquired_time_string + acquired_subsamples
                for current_value in acquired_values:
                    acquired_subsamples = acquired_subsamples + repr(current_value) + ' '
            acquired_value = acquired_values[0]
            
        except (OSError, ValueError) as e:
            print(e)

        acquired_base64 = b''

        self.current_channel = current_channel
        self.acquired_time = acquired_time
        self.acquired_value = acquired_value
        self.acquired_subsamples = acquired_subsamples
        self.acquired_base64 = acquired_base64
        self.current_file = current_file



    def store_file_data(self):

        channel_string = repr(self.current_channel)
        acquired_time_string = repr(self.acquired_time)
        acquired_value_string = repr(self.acquired_value)

        insert_result = -1

        try:
            
            with self.conn.cursor() as cursor :

                try:
                    cursor.execute(self.delete_sql, (acquired_time_string, channel_string) )
                except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                    print(e)

            if not math.isnan(self.acquired_value):
                with self.conn.cursor() as cursor :
                    try:
                        print(acquired_time_string, channel_string, acquired_value_string, self.acquired_subsamples, self.acquired_base64)
                        cursor.execute(self.insert_sql, (acquired_time_string, channel_string, acquired_value_string, self.acquired_subsamples, self.acquired_base64))
                    except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                        print(e)
                    insert_result = cursor.rowcount

        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            print(e)
     
        try:
            if insert_result > -1:
                os.remove(self.dataFilepath + self.current_file)
        except (PermissionError, FileNotFoundError) as e:
            print(e)

        return insert_result


    def run(self):

        while True:

            time.sleep(0.5)

            for channel_index in self.channels:

                insert_failure = False

                files = self.get_filenames(channel = channel_index)        
                print('channel_index', channel_index, 'len(files)', len(files))

                if len(files) > 2 :
                        
                    self.connect_db()
                        
                    for current_file in files:
                            
                        if len(files) > 2:
                                
                            self.retrieve_file_data(current_file)

                            store_result = self.store_file_data()
                            if store_result <= -1: insert_failure = True

                        if not insert_failure:
                            self.commit_transaction()

                    self.close_db_connection()



class ImageFile(File):

    pass



class NumpyFile(DataFile):


    def __init__(self, channels = None, scale_functions = None):

        DataFile.__init__(self, channels = None, scale_functions = None)

        self.channels = channels


    def load_file(self, current_file = None):

        return numpy.load(self.dataFilepath + current_file)
