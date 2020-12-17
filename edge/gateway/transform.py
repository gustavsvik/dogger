#

import time
import math

import gateway.database as db
import gateway.task as ta
import gateway.runtime as rt
import gateway.aislib as ai



class Nmea :


    def __init__(self, prepend = None, append = None) :

        self.prepend = prepend
        self.append = append


    def get_checksum(self, nmea_data) :

        nmea_bytearray = bytes(nmea_data, encoding='utf8')
        checksum = 0
        for i in range(0, len(nmea_bytearray)) :
            if nmea_bytearray[i] != 44 :
                checksum = checksum ^ nmea_bytearray[i]
        checksum_hex = hex(checksum)
        return checksum_hex[2:]


    def from_float(self, multiplier, decimals, value) :

        nmea_string = ''

        try :

            nmea_data = self.prepend
            try :
                nmea_data += "{:.{}f}".format(float(value) * multiplier, decimals)
            except ValueError as e :
                nmea_data += '9999.0'
                rt.logging.exception(e)
            finally :
                nmea_data += self.append
            rt.logging.debug(nmea_data)

            nmea_string = '$' + nmea_data + '*' + self.get_checksum(nmea_data) + '\n'
            print("nmea_string", nmea_string)

        except Exception as e :

            print(e)

        finally :

            return nmea_string


    def from_pos(self, timestamp, latitude, longitude) :

        nmea_data = self.prepend

        try :

            latitude_dir = 'N'
            latitude_abs = abs(float(latitude))
            latitude_sign = float(latitude) / latitude_abs
            if latitude_sign < 0 : latitude_dir = 'S'
            latitude_deg = latitude_abs // 1
            latitude_min = ( latitude_abs - latitude_deg ) * 60

            longitude_dir = 'E'
            longitude_abs = abs(float(longitude))
            longitude_sign = float(longitude) / longitude_abs
            if longitude_sign < 0 : longitude_dir = 'W'
            longitude_deg = longitude_abs // 1
            longitude_min = ( longitude_abs - longitude_deg ) * 60

            datetime_origin = datetime.datetime.fromtimestamp(int(timestamp))
            origin_timestamp = datetime_origin.timetuple()
            hour = origin_timestamp.tm_hour
            min = origin_timestamp.tm_min
            sec = origin_timestamp.tm_sec
            lead_zero = ''
            if longitude_deg < 100 :
                lead_zero = '0'
            timestamp_string = "{:.{}f}".format(hour * 10000 + min * 100 + sec, 2)
            nmea_data += "{:.{}f}".format(latitude_deg * 100 + latitude_min, 7) + ',' + latitude_dir + ',' + lead_zero + "{:.{}f}".format(longitude_deg * 100 + longitude_min, 7) + ',' + longitude_dir + ',' + timestamp_string

        except ValueError as e :
            nmea_data += '9999.0,N,9999.0,E'
            rt.logging.exception(e)
        finally :
            nmea_data += self.append

        nmea_string = '$' + nmea_data + '*' + self.get_checksum(nmea_data) + '\n'
        print('nmea_string', nmea_string)

        return nmea_string


    def aivdm_from_static(self, mmsi, call_sign, vessel_name, ship_type) :

        aivdm_payload = self.prepend

        try :

            aivdm_message = ai.AISStaticAndVoyageReportMessage(mmsi = mmsi, callsign = call_sign, shipname = vessel_name, shiptype = ship_type, imo = 0)
            aivdm_instance = ai.AIS(aivdm_message)
            aivdm_payload += aivdm_instance.build_payload(False)
            rt.logging.debug("aivdm_payload", aivdm_payload)

        except ValueError as e :

            aivdm_payload += ''
            rt.logging.exception(e)

        finally :

            aivdm_payload += self.append


    def aivdm_from_pos(self, mmsi, timestamp, latitude, longitude) :

        aivdm_payload = self.prepend

        try :

            latitude_degs = float(latitude)
            rt.logging.debug("latitude_degs", latitude_degs)
            latitude_min_fraction = int(latitude_degs * 60 * 10000)

            longitude_degs = float(longitude)
            rt.logging.debug("longitude_degs", longitude_degs)
            longitude_min_fraction = int(longitude_degs * 60 * 10000)

            datetime_origin = datetime.datetime.fromtimestamp(int(timestamp))
            origin_timestamp = datetime_origin.timetuple()
            sec = origin_timestamp.tm_sec
            
            aivdm_message = ai.AISPositionReportMessage(mmsi = mmsi, lon = longitude_min_fraction, lat = latitude_min_fraction, ts = sec, status = 15)
            aivdm_instance = ai.AIS(aivdm_message)
            aivdm_payload += aivdm_instance.build_payload(False)
            rt.logging.debug("aivdm_payload", aivdm_payload)

        except ValueError as e :

            aivdm_payload += ''
            rt.logging.exception(e)

        finally :

            aivdm_payload += self.append

        return aivdm_payload


    def aivdm_area_notice_circle_from_pos(self, mmsi, latitude, longitude) :

        aivdm_payload = self.prepend

        try :

            latitude_degs = float(latitude)
            latitude_min_fraction = int(latitude_degs * 60 * 1000)
            rt.logging.debug("latitude_degs", latitude_degs, "latitude_min_fraction", latitude_min_fraction)

            longitude_degs = float(longitude)
            longitude_min_fraction = int(longitude_degs * 60 * 1000)
            rt.logging.debug("longitude_degs", longitude_degs, "longitude_min_fraction", longitude_min_fraction)

            rt.logging.debug("mmsi", mmsi)
            aivdm_message = ai.AISBinaryBroadcastMessageAreaNoticeCircle(mmsi = mmsi, lon_1 = longitude_min_fraction, lat_1 = latitude_min_fraction, radius_1 = 100)
            aivdm_instance = ai.AIS(aivdm_message)
            aivdm_payload += aivdm_instance.build_payload(False)
            rt.logging.debug("aivdm_payload", aivdm_payload)

        except ValueError as e :

            aivdm_payload += ''
            rt.logging.exception(e)

        finally :

            aivdm_payload += self.append

        return aivdm_payload


    def aivdm_atons_from_pos(self, mmsis, latitude, longitude, types, names, virtual_aids, length_offsets, width_offsets) :

        aivdm_payloads = []

        try :

            rt.logging.debug("mmsis", mmsis, "length_offsets", length_offsets, "width_offsets", width_offsets)

            for mmsi, aid_type, name, virtual_aid, length_offset, width_offset in zip(mmsis, types, names, virtual_aids, length_offsets, width_offsets) :

                latitude_degs = float(latitude)
                longitude_degs = float(longitude)

                latitude_degs += float ( length_offset / ( 1 * 1852 ) * 1/60 )
                latitude_min_fraction = int(latitude_degs * 60 * 10000)
                rt.logging.debug("latitude_degs", latitude_degs, "latitude_min_fraction", latitude_min_fraction)

                latitude_factor = math.cos(latitude_degs/180 * math.pi)
                longitude_degs += float ( width_offset / ( latitude_factor * 1852 ) * 1/60 )
                longitude_min_fraction = int(longitude_degs * 60 * 10000)
                rt.logging.debug("longitude_degs", longitude_degs, "longitude_min_fraction", longitude_min_fraction)

                print("mmsi", mmsi, "aid_type", aid_type, "name", name, "longitude_min_fraction", longitude_min_fraction, "latitude_min_fraction", latitude_min_fraction, "virtual_aid", virtual_aid)
                aivdm_message = ai.AISAtonReport(mmsi = mmsi, aid_type = aid_type, name = name, lon = longitude_min_fraction, lat = latitude_min_fraction, virtual_aid = virtual_aid)
                aivdm_instance = ai.AIS(aivdm_message)
                aivdm_payload = aivdm_instance.build_payload(False)
                rt.logging.debug("aivdm_payload", aivdm_payload)

                aivdm_payloads.append(aivdm_payload)

        except ValueError as e :

            rt.logging.exception(e)

        finally :

            pass

        return aivdm_payloads



class Accumulate(ta.ProcessDataTask) :


    def __init__(self, channels = None, start_delay = None, gateway_database_connection = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.gateway_database_connection = gateway_database_connection

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        ta.ProcessDataTask.__init__(self)

        self.sql = db.SQL(gateway_database_connection = self.gateway_database_connection, config_filepath = self.config_filepath, config_filename = self.config_filename)

        self.previous_minute = -1
        self.previous_timestamp = int(time.mktime(time.localtime()))


    def run(self) :

        time.sleep(self.start_delay)

        while (True) :

            time.sleep(0.1)

            current_localtime = time.localtime()
            current_hour =  current_localtime.tm_hour
            current_minute = current_localtime.tm_min
            current_second = current_localtime.tm_sec
            current_timestamp = int(time.mktime(time.localtime()))

            if current_timestamp - self.previous_timestamp > 60 :
                current_timestamp = self.previous_timestamp + 60
                current_minute = self.previous_minute + 1
            
            if current_second == 0 and current_minute != self.previous_minute :

                for channel_index in self.channels: 

                    accumulated_min_samples = 0
                    accumulated_min_value = 0.0
                    accumulated_hour_samples = 0
                    accumulated_hour_value = 0.0
                    accumulated_text = ''
                    accumulated_binary = b''
                    
                    try :

                        self.sql.connect_db()

                        accumulated_bin_size = 60
                        accumulated_bin_end_time = current_timestamp - accumulated_bin_size

                        sql_get_minute_data = "SELECT ACQUIRED_TIME,ACQUIRED_VALUE FROM t_acquired_data WHERE CHANNEL_INDEX=%s AND ACQUIRED_TIME<%s AND ACQUIRED_TIME>=%s ORDER BY ACQUIRED_TIME DESC"

                        with self.sql.conn.cursor() as cursor :
                            cursor.execute(sql_get_minute_data, [channel_index, accumulated_bin_end_time, accumulated_bin_end_time - 60] )
                            results = cursor.fetchall()
                            for row in results:
                                acquired_time = row[0]
                                acquired_value = row[1]
                                if not -9999.01 < acquired_value < -9998.99 :
                                    accumulated_min_value += acquired_value
                                    accumulated_min_samples += 1

                        accumulated_min_mean = 0.0
                        if accumulated_min_samples > 0 :
                            accumulated_min_mean = accumulated_min_value / accumulated_min_samples

                        sql_insert_accumulated_60 = "INSERT INTO t_accumulated_data (CHANNEL_INDEX,ACCUMULATED_BIN_END_TIME,ACCUMULATED_BIN_SIZE,ACCUMULATED_NO_OF_SAMPLES,ACCUMULATED_VALUE,ACCUMULATED_TEXT,ACCUMULATED_BINARY) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                        with self.sql.conn.cursor() as cursor :
                            try:
                                cursor.execute(sql_insert_accumulated_60, [channel_index, accumulated_bin_end_time, accumulated_bin_size, accumulated_min_samples, accumulated_min_mean, accumulated_text, accumulated_binary] )
                            except pymysql.err.IntegrityError as e:
                                rt.logging.exception(e)

                        accumulated_min_samples = 0
                        accumulated_min_value = 0.0

                        if current_minute == 1 :

                            accumulated_bin_size = 3600

                            sql_get_hour_data = 'SELECT ACQUIRED_TIME,ACQUIRED_VALUE FROM t_acquired_data WHERE CHANNEL_INDEX=%s AND ACQUIRED_TIME<%s AND ACQUIRED_TIME>=%s ORDER BY ACQUIRED_TIME DESC'
                            with self.sql.conn.cursor() as cursor :
                                cursor.execute(sql_get_hour_data, [channel_index, accumulated_bin_end_time, accumulated_bin_end_time - 3600] )
                                results = cursor.fetchall()
                                for row in results:
                                    acquired_time = row[0]
                                    acquired_value = row[1]
                                    if not -9999.01 < acquired_value < -9998.99 :
                                        accumulated_hour_value += acquired_value
                                        accumulated_hour_samples += 1

                            accumulated_hour_mean = 0.0
                            if accumulated_hour_samples > 0 :
                                accumulated_hour_mean = accumulated_hour_value / accumulated_hour_samples
                                
                            sql_insert_accumulated_3600 = "INSERT INTO t_accumulated_data (CHANNEL_INDEX,ACCUMULATED_BIN_END_TIME,ACCUMULATED_BIN_SIZE,ACCUMULATED_NO_OF_SAMPLES,ACCUMULATED_VALUE,ACCUMULATED_TEXT,ACCUMULATED_BINARY) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                            with self.sql.conn.cursor() as cursor :
                                try:
                                    cursor.execute(sql_insert_accumulated_3600, [channel_index, accumulated_bin_end_time,accumulated_bin_size, accumulated_hour_samples, accumulated_hour_mean, accumulated_text, accumulated_binary] )
                                except pymysql.err.IntegrityError as e:
                                    rt.logging.exception(e)

                            accumulated_hour_samples = 0
                            accumulated_hour_value = 0.0

                    finally:
                    
                        self.sql.close_db_connection()

                        
                self.previous_minute = current_minute
                self.previous_timestamp = current_timestamp
