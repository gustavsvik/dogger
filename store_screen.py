import time
import pymysql
import os
import math
import base64

import timefiles
import metadata


config = metadata.Configure(filename = 'conf_screen.ini')
env = config.get()

while (True):
    
    time.sleep(0.5)

    for channel_index in {600}:
        
        files = []
                    
        files = timefiles.get_all_files(env['STORE_PATH'], ['jpeg'], channel_index)
        insert_failure = False
        open_failure = False

        if len(files) > 0:

            try:
                
                conn = pymysql.connect(host = env['STORE_DATABASE_HOST'], user = env['STORE_DATABASE_USER'], passwd = env['STORE_DATABASE_PASSWD'], db = env['STORE_DATABASE_DB'], autocommit = True)

                for current_file in files:

                    acquired_time = timefiles.get_file_local_datetime(current_file, datetime_pattern = '%Y%m%d%H%M%S')
                    acquired_time_string = repr(acquired_time)

                    if len(files) > 0: 

                        acquired_value = -9999.0
                        acquired_subsamples = ""
                        acquired_base64 = b''

                        if not math.isnan(acquired_value):
                            with conn.cursor() as cursor :
                                try :
                                    with open(current_file, "rb") as image_file :
                                        acquired_base64 = base64.b64encode(image_file.read())
                                except OSError as e :
                                    open_failure = True
                                    print(e)
                                if not open_failure :
                                    sql = "INSERT INTO t_acquired_data (ACQUIRED_TIME,CHANNEL_INDEX,ACQUIRED_VALUE,ACQUIRED_SUBSAMPLES,ACQUIRED_BASE64) VALUES (%s,%s,%s,%s,%s)"
                                    try:
                                        cursor.execute(sql, (acquired_time_string,channel_index,acquired_value,acquired_subsamples,acquired_base64))
                                    except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                                        print(e)
                                    result = cursor.rowcount
                                    if result <= -1: insert_failure = True
                        try:
                            if not insert_failure :
                                os.remove(current_file)
                        except (PermissionError, FileNotFoundError, OSError) as e:
                            print(e)

                    else:
                        
                        time.sleep(0.5)
                            
                if not insert_failure: conn.commit()
            
            except (pymysql.err.OperationalError, pymysql.err.Error) as e:
                print(e)

            finally:

                try:
                    conn.close()
                except pymysql.err.Error as e:
                    print(e)
