import pymysql
import numpy
import time
import dogger.metadata

#FILE_PATH = "../../data/files/"
#FILE_PATH = "C:/Z/THISBUSINESS/Energilab/PROJECTS/logging/data/files/"

config = dogger.metadata.Configure()
FILE_PATH = config.getDataFilePath()


start_time = -9999

acquired_time = -9999
acquired_value = -9999.0

while True:

    time.sleep(0.5)

    for channel_index in {39}:
        
        update_failure = False

        try:
                    
            conn = pymysql.connect(host='localhost', user='root', passwd='root', db='test', autocommit=True)

            acquired_times_string = ""

            sql_get_values = "SELECT AD.ACQUIRED_TIME,AD.ACQUIRED_VALUE FROM T_ACQUIRED_DATA AD WHERE AD.CHANNEL_INDEX=" + repr(channel_index) + " AND AD.STATUS=-1"
            print("sql_get_values",sql_get_values)
            with conn.cursor() as cursor :
                try:
                    cursor.execute(sql_get_values)
                except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                    print(e)
                results = cursor.fetchall()
                for row in results:
                    acquired_time = row[0]
                    acquired_value = row[1]
                    print('Channel: ', channel_index, ', Value: ', acquired_value, ', Timestamp: ', acquired_time)
                    acquired_times_string += repr(acquired_time) + ","
                    filename = repr(channel_index) + "_" + repr(acquired_time)
                    numpy.save(FILE_PATH+filename, float(acquired_value))
                    
            if len(results)>0:
                sql = "UPDATE T_ACQUIRED_DATA SET STATUS=0 WHERE STATUS=-1 AND CHANNEL_INDEX=" + repr(channel_index) + " AND ACQUIRED_TIME IN (" + acquired_times_string[0:len(acquired_times_string)-1] + ")"
                print("print(sql) ", sql)
                with conn.cursor() as cursor :
                    try:
                        cursor.execute(sql)
                    except (pymysql.err.IntegrityError, pymysql.err.InternalError) as e:
                        print(e)
                    result = cursor.rowcount
                    print("result", result)
                    if result <= -1: update_failure = True

                if not update_failure:
                    conn.commit()
                    print('committed')

        except (pymysql.err.OperationalError, pymysql.err.Error) as e:

            print(e)


        finally:
                    
            try:
                conn.close()
            except pymysql.err.Error as e:
                print(e)


        if acquired_time != -9999 and abs(acquired_value - 1.0) < 0.1:
            start_time = acquired_time
            acquired_time = -9999
            acquired_value = -9999.0
        
        if start_time != -9999 and time.time() > start_time + 60:
            filename = repr(channel_index) + "_" + repr(time.time())
            numpy.save(FILE_PATH+filename, float(0.0))
            start_time = -9999
