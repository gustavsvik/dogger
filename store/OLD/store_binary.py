import glob
import time
import datetime
import pymysql
import numpy
import sys
import os
import math
import base64
import dogger.metadata


config = dogger.metadata.Configure()
FILE_PATH = config.getDataFilePath()


while (True):
    
    time.sleep(0.5)

    for channel_index in {140,160}:
        
        files = []
        
        with os.scandir(FILE_PATH) as it:
            for entry in it:
                if entry.name.startswith(repr(channel_index) + '_') and entry.is_file():
                    files.append(entry.name)

        insert_failure = False

        print('channel_index', channel_index, 'len(files)', len(files))

        if len(files) > 0:

            try:
                
                conn = pymysql.connect(host='localhost', user='root', passwd='root', db='test', autocommit=True)

                for current_file in files:

                    position = current_file.find("_")
                    acquired_time_string = current_file[position+1:position+1+10]
                    acquired_time = int(acquired_time_string)
                    print('acquired_time', acquired_time)

                    #print("print(int(time.time())) ", int(time.time()))

                    if len(files) > 2: #if acquired_time < int(time.time()) - 2*1.0/1.0 :

                        #print(current_file)
                        acquired_value = -9999.0
                        acquired_subsamples = ""
                        #try:
                            #acquired_values = numpy.load(current_file)
                            #print("print(acquired_values) ", acquired_values)
                            #if len(acquired_values) > 1:
                            #    acquired_subsamples = acquired_time_string + acquired_subsamples
                            #    for current_value in acquired_values:
                            #        acquired_subsamples = acquired_subsamples + repr(current_value) + " "
                            #        
                            #acquired_value = acquired_values[0]
                            #print(acquired_value)
                        #except OSError as e:
                            #print(e)
                        acquired_base64 = b''

                        #with conn.cursor() as cursor :
                        #    sql = "DELETE FROM T_ACQUIRED_DATA WHERE ACQUIRED_TIME=" + acquired_time_string + " AND CHANNEL_INDEX=" + repr(channel_index)
                        #    print("print(sql) ", sql)
                        #    cursor.execute(sql)

                        if not math.isnan(acquired_value):
                            with conn.cursor() as cursor :
                                with open(FILE_PATH + current_file, "rb") as image_file:
                                    acquired_base64 = base64.b64encode(image_file.read())
                                sql = "INSERT INTO T_ACQUIRED_DATA (ACQUIRED_TIME,CHANNEL_INDEX,ACQUIRED_VALUE,ACQUIRED_SUBSAMPLES,ACQUIRED_BASE64) VALUES (%s,%s,%s,%s,%s)"
                                try:
                                    cursor.execute(sql, (acquired_time_string,channel_index,acquired_value,acquired_subsamples,acquired_base64))
                                except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                                    print(e)
                                result = cursor.rowcount
                                if result <= -1: insert_failure = True
                        try:
                            if not insert_failure:
                                os.remove(FILE_PATH + current_file)
                        except (PermissionError, FileNotFoundError) as e:
                            print(e)

                        #oldest_time = 0
                        #sql_get_oldest_time = "SELECT MIN(ACQUIRED_TIME) FROM T_ACQUIRED_DATA WHERE CHANNEL_INDEX=" + repr(channel_index)
                        #with conn.cursor() as cursor :
                        #    cursor.execute(sql_get_oldest_time)
                        #    results = cursor.fetchall()
                        #    for row in results:
                        #        oldest_time = row[0]
                        #stored_timespan = int(time.time())-int(oldest_time)
                        ##print("stored_timespan", stored_timespan)
                        #if stored_timespan > 10000 and oldest_time > 0:
                        #    sql_clean_data = "DELETE FROM T_ACQUIRED_DATA WHERE CHANNEL_INDEX=" + repr(channel_index) + " AND ACQUIRED_TIME BETWEEN " + repr(oldest_time) + " AND " + repr(oldest_time+1000)
                        #    #print("sql_clean_data", sql_clean_data)
                        #    with conn.cursor() as cursor :
                        #            cursor.execute(sql_clean_data)

                    else:
                        
                        time.sleep(0.5)
                            
                if not insert_failure: conn.commit()
            
            except (pymysql.err.OperationalError, pymysql.err.Error) as e:
                print(e)
                #print("Unexpected error:", sys.exc_info()[0])

            finally:

                try:
                    conn.close()
                except pymysql.err.Error as e:
                    print(e)

