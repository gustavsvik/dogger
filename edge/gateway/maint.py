#

import time
import datetime
import calendar
import math
import struct
from contextlib import closing
from socket import gaierror, socket, AF_INET, SOCK_DGRAM
import pymysql

import gateway.task as ta

import gateway.persist as ps
import gateway.runtime as rt
import gateway.inet as it



class CloudDBPartition(it.HttpMaint):


    def __init__(self, start_delay = None, ip_list = None, http_scheme = None, maint_api_url = None, max_connect_attempts = None, new_partition_name_date = None, new_partition_timestamp = None, oldest_kept_partition_name_date = None, config_filepath = None, config_filename = None):

        self.start_delay = start_delay
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.maint_api_url = maint_api_url
        self.max_connect_attempts = max_connect_attempts
        self.new_partition_name_date = new_partition_name_date
        self.new_partition_timestamp = new_partition_timestamp
        self.oldest_kept_partition_name_date = oldest_kept_partition_name_date

        #self.transmit_rate = math.inf
        #self.channels = set()

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        it.HttpMaint.__init__(self)



class PartitionDatabase(ta.MaintenanceTask) :


    def __init__(self) :

        self.max_connect_attempts = 50

        ta.MaintenanceTask.__init__(self)


    def run(self) :

        previous_monthday = -1

        while (True) :

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

                datetime_midnight = datetime.datetime(current_year, current_month, current_monthday) + datetime.timedelta(days = 1)
                datetime_tomorrow_midnight = datetime_midnight + datetime.timedelta(days = 1)
                new_partition_timestamp = str(calendar.timegm(datetime_tomorrow_midnight.utctimetuple()))
                new_partition_name_date = datetime_midnight.strftime('%Y%m%d')
                datetime_last_midnight_to_keep = datetime_midnight - datetime.timedelta(days = self.keep_partitions_horizon)
                oldest_kept_partition_name_date = datetime_last_midnight_to_keep.strftime('%Y%m%d')

                self.partition_database(new_partition_name_date, new_partition_timestamp, oldest_kept_partition_name_date)

                previous_monthday = current_monthday



class PartitionEdgeDatabase(PartitionDatabase) :


    def __init__(self, start_delay = None, ip_list = None, http_scheme = None, keep_partitions_horizon = None, config_filepath = None, config_filename = None) :

        self.start_delay = start_delay
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.keep_partitions_horizon = keep_partitions_horizon

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        PartitionDatabase.__init__(self)

        self.sql = ps.SQL(config_filepath = self.config_filepath, config_filename = self.config_filename)


    def partition_database(self, new_partition_name_date = None, new_partition_timestamp = None, oldest_kept_partition_name_date = None) :

        try:

            self.sql.connect_db()
            db_name = self.sql.gateway_database_connection['db']

            sql_reorganize_partitions = "ALTER TABLE " + db_name + ".t_acquired_data REORGANIZE PARTITION acquired_time_max INTO ( PARTITION acquired_time_" + new_partition_name_date + " VALUES LESS THAN (" + new_partition_timestamp + "), PARTITION acquired_time_max VALUES LESS THAN (MAXVALUE) )"
            rt.logging.debug(sql_reorganize_partitions)
            with self.sql.conn.cursor() as cursor :
                try:
                    cursor.execute(sql_reorganize_partitions)
                except pymysql.err.InternalError as e:
                    rt.logging.exception(e)

            sql_get_all_date_partition_strings = "SELECT PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA='" + db_name + "' AND PARTITION_NAME IS NOT NULL AND LOWER(PARTITION_NAME)<>'acquired_time_max' ORDER BY PARTITION_NAME"
            rt.logging.debug(sql_get_all_date_partition_strings)
            all_partition_date_strings = []
            with self.sql.conn.cursor() as cursor :
                try:
                    cursor.execute(sql_get_all_date_partition_strings)
                except pymysql.err.InternalError as e:
                    rt.logging.exception(e)
                results = cursor.fetchall()
                for row in results:
                    date_partition_string = row[0]
                    all_partition_date_strings.append(date_partition_string[-8:])

            for partition_date_string in all_partition_date_strings :
                if partition_date_string < oldest_kept_partition_name_date :
                    sql_drop_old_partition = "ALTER TABLE " + db_name + ".t_acquired_data DROP PARTITION acquired_time_" + partition_date_string
                    rt.logging.debug(sql_drop_old_partition)
                    with self.sql.conn.cursor() as cursor :
                        try:
                            cursor.execute(sql_drop_old_partition)
                        except pymysql.err.InternalError as e:
                            rt.logging.exception(e)

        except (pymysql.err.OperationalError, pymysql.err.Error) as e:
            rt.logging.exception(e)

        finally:
            try:
                self.sql.close_db_connection()
            except pymysql.err.Error as e:
                rt.logging.exception(e)



class PartitionCloudDatabase(PartitionDatabase) :


    def __init__(self, start_delay = None, ip_list = None, http_scheme = None, maint_api_url = None, keep_partitions_horizon = None, config_filepath = None, config_filename = None) :

        self.start_delay = start_delay
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.maint_api_url = maint_api_url
        self.keep_partitions_horizon = keep_partitions_horizon

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        PartitionDatabase.__init__(self)


    def partition_database(self, new_partition_name_date = None, new_partition_timestamp = None, oldest_kept_partition_name_date = None) :

        rt.logging.debug(self.start_delay, self.ip_list, self.maint_api_url, new_partition_name_date, new_partition_timestamp, oldest_kept_partition_name_date)
        http = CloudDBPartition(start_delay = self.start_delay, ip_list = self.ip_list, http_scheme = self.http_scheme, maint_api_url = self.maint_api_url, max_connect_attempts = self.max_connect_attempts, new_partition_name_date = new_partition_name_date, new_partition_timestamp = new_partition_timestamp, oldest_kept_partition_name_date = oldest_kept_partition_name_date)

        if self.ip_list is not None :
            for current_ip in self.ip_list :
                rt.logging.debug("current_ip", current_ip)
                res = http.partition_database(current_ip)



class NetworkTime(ta.MaintenanceTask) :


    def __init__(self, start_delay = None, ip_list = None, http_scheme = None, ntp_url = None, ntp_port = None, adjust_interval = None, max_connect_attempts = None, config_filepath = None, config_filename = None) :

        self.start_delay = start_delay
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.ntp_url = ntp_url
        self.ntp_port = ntp_port
        self.adjust_interval = adjust_interval
        self.max_connect_attempts = max_connect_attempts

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        ta.MaintenanceTask.__init__(self)


    def get_network_time(self) :

        try :

            with closing(socket(AF_INET, SOCK_DGRAM)) as s :

                t0 = time.time()

                s.sendto(('\x23' + 47 * '\0').encode(), (self.ntp_url, self.ntp_port))   # see https://stackoverflow.com/a/26938508
                msg, address = s.recvfrom(1024)                   # and https://stackoverflow.com/a/33436061
                unpacked = struct.unpack("!12I", msg[0:struct.calcsize("!12I")])  # ! => network (= big-endian), 12 => returns 12-tuple, I => unsigned int
                t3 = time.time()
                t1 = unpacked[8] + float(unpacked[9]) / 2**32 - 2208988800     # see https://tools.ietf.org/html/rfc5905#page-19
                t2 = unpacked[10] + float(unpacked[11]) / 2**32 - 2208988800   # and https://tools.ietf.org/html/rfc5905#page-13
                offset = ((t1 - t0) + (t2 - t3)) / 2    # https://en.wikipedia.org/wiki/Network_Time_Protocol#Clock_synchronization_algorithm
                roundtrip = (t3 -  t0) - (t2 - t1)

                print("Local computer time (t0)                               %.3f" % t0)
                print("NTP server time (t1, receive timestamp)                %.3f" % t1)
                print("NTP server time (t2, transmit timestamp)               %.3f" % t2)
                print("Local computer time (t3)                               %.3f" % t3)
                print("Offset                                                 %.1f ms" % (offset * 1000) )
                print("Local -> NTP server -> local roundtrip time            %.1f ms" % (roundtrip * 1000) )
                print("New local computer time                                %.3f" % (t3 + offset) )

                self.current_system_time = t3
                self.current_system_time_offset = offset

        except gaierror as e :
            rt.logging.exception(e)

        dt = None
        try :
            dt = datetime.datetime.utcfromtimestamp(self.current_system_time + self.current_system_time_offset)
        except OSError as e :
            rt.logging.exception(e)

        return dt


    def adjust_win_system_time(self, dt) :

        import win32api  # pip install pywin32

        win32api.SetSystemTime(dt.year, dt.month, dt.isocalendar()[2], dt.day, dt.hour, dt.minute, dt.second, int(dt.microsecond / 1000) )


    def run(self) :

        import time, numpy

        adj_time = self.get_network_time()
        if adj_time is not None :
            self.adjust_win_system_time(adj_time)

        acq_prev_time = numpy.float64(time.time())

        diff_time = 0

        while True :

            acq_finish_time = numpy.float64(time.time())
            diff_time += acq_finish_time - acq_prev_time
            acq_prev_time = acq_finish_time

            if diff_time > self.adjust_interval :
                adj_time = self.get_network_time()
                if adj_time is not None :
                    self.adjust_win_system_time(adj_time)
                    diff_time = 0

            time.sleep(0.2)
