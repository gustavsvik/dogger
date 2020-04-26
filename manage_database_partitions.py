import time
import datetime
import calendar
import pymysql

import runtime as rt
from metadata import Configure

config = Configure()
env = config.get()

previous_monthday = -1

while (True):

    time.sleep(0.5)

    current_datetime = datetime.datetime.utcnow()
    current_timestamp = current_datetime.timetuple()
    current_year = current_timestamp.tm_year
    current_month = current_timestamp.tm_mon
    current_monthday = current_timestamp.tm_mday
    current_hour =  current_timestamp.tm_hour
    current_minute = current_timestamp.tm_min
    current_second = current_timestamp.tm_sec

    if current_minute == 50 and current_hour == 23 and current_monthday != previous_monthday :

        datetime_midnight = datetime.datetime(current_year, current_month, current_monthday) + datetime.timedelta(days=1)
        #print('datetime_midnight', datetime_midnight)
        datetime_tomorrow_midnight = datetime_midnight + datetime.timedelta(days=1)
        #print('datetime_tomorrow_midnight', datetime_tomorrow_midnight)
        new_partition_timestamp_string = str(calendar.timegm(datetime_tomorrow_midnight.utctimetuple()))
        #print('new_partition_timestamp_string', new_partition_timestamp_string)
        new_partition_name_string = datetime_midnight.strftime('%Y%m%d')
        #print('new_partition_name_string', new_partition_name_string)

        try:

            conn = pymysql.connect(host=env['STORE_DATABASE_HOST'], user=env['STORE_DATABASE_USER'], passwd=env['STORE_DATABASE_PASSWD'], db=env['STORE_DATABASE_DB'], autocommit=True)

            sql_reorganize_partitions = "ALTER TABLE t_acquired_data REORGANIZE PARTITION acquired_time_max INTO ( PARTITION acquired_time_" + new_partition_name_string + " VALUES LESS THAN (" + new_partition_timestamp_string + "), PARTITION acquired_time_max VALUES LESS THAN (MAXVALUE) )"
            with conn.cursor() as cursor :
                try:
                    cursor.execute(sql_reorganize_partitions)
                except pymysql.err.InternalError as e:
                    rt.logging.debug(e)

            datetime_last_midnight_to_keep = datetime_midnight - datetime.timedelta(days=10)
            #print('datetime_last_midnight_to_keep', datetime_last_midnight_to_keep)
            last_partition_date_string_to_keep = datetime_last_midnight_to_keep.strftime('%Y%m%d')
            #print('last_partition_date_string_to_keep', last_partition_date_string_to_keep)

            sql_get_all_date_partition_strings = "SELECT PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS WHERE PARTITION_NAME IS NOT NULL AND LOWER(PARTITION_NAME)<>'acquired_time_max' ORDER BY PARTITION_NAME"
            all_partition_date_strings = []
            with conn.cursor() as cursor :
                try:
                    cursor.execute(sql_get_all_date_partition_strings)
                except pymysql.err.InternalError as e:
                    rt.logging.debug(e)
                results = cursor.fetchall()
                for row in results:
                    date_partition_string = row[0]
                    #print('date_partition_string', date_partition_string)
                    #print('date_partition_string[-8:]', date_partition_string[-8:])
                    all_partition_date_strings.append(date_partition_string[-8:])
                    #print('all_partition_date_strings', all_partition_date_strings)

            for partition_date_string in all_partition_date_strings :
                if partition_date_string < last_partition_date_string_to_keep :
                    sql_drop_old_partition = "ALTER TABLE t_acquired_data DROP PARTITION acquired_time_" + partition_date_string
                    #print('sql_drop_old_partition', sql_drop_old_partition)
                    with conn.cursor() as cursor :
                        try:
                            cursor.execute(sql_drop_old_partition)
                        except pymysql.err.InternalError as e:
                            rt.logging.debug(e)

        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            rt.logging.debug(e)

        finally:
            try:
                conn.close()
            except pymysql.err.Error as e:
                rt.logging.debug(e)

        previous_monthday = current_monthday
