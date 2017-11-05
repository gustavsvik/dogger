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

#FILE_PATH = "../../data/files/"
#FILE_PATH = "C:/Z/THISBUSINESS/Energilab/PROJECTS/logging/data/files/"

config = dogger.metadata.Configure()
FILE_PATH = config.getDataFilePath()

channel_index = 600

while (True):
    
    time.sleep(0.5)

    #"{:%Y-%m-%d}".format(datetime.date.today())
    #date_string = datetime.datetime.now().strftime('%Y-%m-%d')
    #time_string = datetime.datetime.now().strftime('%H-%M-%S')
    #print(time_string)
    
    folder_pattern = FILE_PATH + 'screenshots/*'
    folders = glob.glob(folder_pattern)
    #print("folder_pattern", folder_pattern)
    #print("folders", folders)

    for folder_string in folders:
        
        insert_failure = False

        date_string = folder_string[-10:]
        #print('date_string', date_string)
        file_pattern = folder_string + '/1/'
        pattern = file_pattern + date_string + '_*' # + time_string + '-*'
        files = glob.glob(pattern)
        #print("channel_index ", channel_index)
        #print("file_pattern", file_pattern)
        #print("files", files)
        #print("print(len(files)) ", len(files))


        if len(files) > 0:

            try:
                
                conn = pymysql.connect(host='localhost', user='root', passwd='root', db='test', autocommit=False)

                for current_file in files:

                    current_file_name = current_file[-28:]
                    print('current_file_name', current_file_name)
                    position = current_file.find("_", len(FILE_PATH+'screenshots/'))
                    acquired_datetime_string = current_file[position+1:position+1+8]
                    print('acquired_datetime_string', acquired_datetime_string)
                    local_datetime = datetime.datetime.strptime(date_string + ' ' + acquired_datetime_string, '%Y-%m-%d %H-%M-%S')
                    acquired_time = int(time.mktime(local_datetime.timetuple()))
                    acquired_time_string = repr(acquired_time)
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
                                with open(current_file, "rb") as image_file:
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
                                os.remove(current_file)
                                os.remove(folder_string + '/5/' + current_file_name)
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

