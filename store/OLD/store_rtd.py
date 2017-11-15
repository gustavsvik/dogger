import glob
import time
import pymysql
import numpy
import sys
import os
import math
import dogger.metadata

#FILE_PATH = "../../data/files/"
#FILE_PATH = "C:/Z/THISBUSINESS/Energilab/PROJECTS/logging/data/files/"

config = dogger.metadata.Configure()
FILE_PATH = config.getDataFilePath()

while True:

    time.sleep(0.5)

    for channel_index in {40}:
                            
        files = []
        
        with os.scandir(FILE_PATH) as it:
            for entry in it:
                if entry.name.startswith(repr(channel_index) + '_') and entry.is_file():
                    files.append(entry.name)

        insert_failure = False

        print('channel_index', channel_index, 'len(files)', len(files))

        if len(files) > 0 :

            try:
                #print("channel_index ", channel_index)

                conn = pymysql.connect(host='localhost', user='root', passwd='root', db='test', autocommit=True)

                for current_file in files:
                    
                    position = current_file.find("_")
                    acquired_time_string = current_file[position+1:position+1+10]
                    acquired_time = int(acquired_time_string)
                    #print("print(acquired_time) ", acquired_time)
                    #print("print(int(time.time())) ", int(time.time()))

                    if len(files) > 2: #int(time.time()) + 60*60*24*30 :

                        #print(current_file)
                        acquired_value = -9999.0
                        acquired_subsamples = ""
                        try:
                            acquired_values = numpy.load(FILE_PATH + current_file)
                            #print("print(acquired_values) ", acquired_values)
                            if len(acquired_values) > 1:
                                acquired_subsamples = acquired_time_string + acquired_subsamples
                                for current_value in acquired_values:
                                    acquired_subsamples = acquired_subsamples + repr(current_value) + " "
                                    
                            acquired_value = acquired_values[0]
                            #print(acquired_value)
                        except (OSError, ValueError) as e:
                            print(e)
                        acquired_base64 = b''

                        with conn.cursor() as cursor :
                            sql = "DELETE FROM T_ACQUIRED_DATA WHERE ACQUIRED_TIME=%s AND CHANNEL_INDEX=%s"
                            #print("print(sql) ", sql)
                            try:
                                cursor.execute(sql, (acquired_time_string,channel_index) )
                            except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                                print(e)

                        if not math.isnan(acquired_value):
                            with conn.cursor() as cursor :
                                #sql = "INSERT INTO T_ACQUIRED_DATA (ACQUIRED_TIME,CHANNEL_INDEX,ACQUIRED_VALUE,ACQUIRED_SUBSAMPLES,ACQUIRED_BASE64) VALUES (" + acquired_time_string + "," + repr(channel_index) + "," + repr(acquired_value) + "," + repr(acquired_subsamples) + "," + repr(acquired_base64) + ")"
                                sql = "INSERT INTO T_ACQUIRED_DATA (ACQUIRED_TIME,CHANNEL_INDEX,ACQUIRED_VALUE,ACQUIRED_SUBSAMPLES,ACQUIRED_BASE64) VALUES (%s,%s,%s,%s,%s)"
                                try:
                                    print(acquired_time_string, channel_index, acquired_value, acquired_subsamples, acquired_base64)
                                    cursor.execute(sql, (acquired_time_string,channel_index,str(acquired_value),acquired_subsamples,acquired_base64))
                                    #cursor.execute(sql)
                                except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                                    print(e)
                                result = cursor.rowcount
                                #print("print(result) ", result)
                                if result <= -1: insert_failure = True
                               
                        try:
                            if not insert_failure: os.remove(FILE_PATH + current_file)
                        except (PermissionError, FileNotFoundError) as e:
                            print(e)
                            
                if not insert_failure: conn.commit()
            
            except (pymysql.err.OperationalError, pymysql.err.Error) as e:
                print(e)
                #print("Unexpected error:", sys.exc_info()[0])

            finally:
            
                try:
                    conn.close()
                except pymysql.err.Error as e:
                    print(e)
                
