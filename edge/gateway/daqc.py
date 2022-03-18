#

import time
import shutil
import os
import sys
import io
import tempfile
import ctypes
import base64
import socket
import struct

try : import pyscreenshot as ImageGrab
except ImportError: pass
try : import numpy
except ImportError: pass
try : import pyais
except ImportError: pass
try : import serial
except ImportError: pass
try : import serial.tools.list_ports
except ImportError: pass
try : from cryptography.fernet import Fernet
except ImportError: pass
try : import cv2
except ImportError: pass
try : from netCDF4 import Dataset as NetCDFFile
except ImportError: pass

import gateway.transform as tr
import gateway.device as dv
import gateway.runtime as rt
import gateway.link as li
import gateway.persist as ps
import gateway.api as ap
import gateway.utils as ut



class UdpHttp(ap.UdpReceive) :


    def __init__(self):

        ap.UdpReceive.__init__(self)


    def upload_data(self, channels = set(), timestamps = None, values = None, byte_strings = None) :

        if self.channels is not None :

            common_channels = set()
            if self.channels == set() :
                common_channels = channels
            else :
                common_channels = channels.intersection(self.channels)

            try :
                http = li.DirectUpload(channels = common_channels, start_delay = self.start_delay, max_connect_attempts = self.max_connect_attempts, http_scheme = self.http_scheme, config_filepath = self.config_filepath, config_filename = self.config_filename)
                for current_ip in self.ip_list :
                    res = http.send_request(start_time = -9999, end_time = -9999, duration = 10, unit = 1, delete_horizon = 3600, ip = current_ip)
                    data_string = tr.delimited_string_from_lists(common_channels, timestamps, values, byte_strings)
                    #data_string = str(channel) + ';' + str(sample_secs) + ',' + str(data_value) + ',,' + byte_string.decode() + ',;'
                    rt.logging.debug("data_string", data_string, "current_ip", current_ip)
                    res = http.set_requested(data_string, ip = current_ip)
            except PermissionError as e :
                rt.logging.exception(e)



class UdpValueHttp(UdpHttp) :


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, ip_list = None, http_scheme = None, port = None, max_connect_attempts = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.port = port
        self.max_connect_attempts = max_connect_attempts

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        UdpHttp.__init__(self)


    def run(self):

        time.sleep(self.start_delay)

        while True :

            #time.sleep(1/self.transmit_rate)
            data, address = self.socket.recvfrom(4096)
            rt.logging.debug("data", data, "len(data)", len(data))

            no_of_data_records = len(data) // 10

            channels = []
            timestamps = []
            values = []
            byte_strings = []

            for index in range(no_of_data_records) :
                single_data_record = data[index * 10 : (index+1) * 10]
                unpacked_record = struct.unpack('>HIf', single_data_record)
                rt.logging.debug("unpacked_record", unpacked_record)
                channels.append(int(unpacked_record[0]))
                timestamps.append(int(unpacked_record[1]))
                values.append(float(unpacked_record[2]))
                byte_strings.append(b'')

            self.upload_data(channels, timestamps, values, byte_strings)



class UdpBytesHttp(UdpHttp) :


    def __init__(self, channels = None, start_delay = None, transmit_rate = None, ip_list = None, http_scheme = None, port = None, max_connect_attempts = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.transmit_rate = transmit_rate
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.port = port
        self.max_connect_attempts = max_connect_attempts

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        UdpHttp.__init__(self)


    def run(self):

        time.sleep(self.start_delay)

        while True :

            #time.sleep(1/self.transmit_rate)
            data, address = self.socket.recvfrom(4096)
            rt.logging.debug("data", data, "len(data)", len(data))
            values = struct.unpack_from('>HI', data, offset = 0)
            rt.logging.debug("values", values)
            byte_string = struct.unpack_from( '{}s'.format(len(data) - 6), data[6:len(data)], offset = 0)
            rt.logging.debug("channel", int(values[0]), "timestamp", int(values[1]), "encrypted_string", byte_string[0])
            crypto_key = b'XUFA58vllD2n41e7NZDZkyPiUCECkxFsBjF_HaKlIrI='
            fernet = Fernet(crypto_key)
            decrypted_string = fernet.decrypt(byte_string[0])
            rt.logging.debug("decrypted_string", decrypted_string)
            rt.logging.debug(" ")
            replaced_byte_string = tr.armor_separators_csv(decrypted_string)
            rt.logging.debug("replaced_byte_string", replaced_byte_string)
            self.upload_data(int(values[0]), int(values[1]), -9999.0, replaced_byte_string)



class StaticFileNmeaFile(ps.IngestFile) :


    def __init__(self, channels = None, start_delay = None, sample_rate = None, concatenate = None, static_file_path_name = None, file_path = None, archive_file_path = None, nmea_prepend = None, nmea_append = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.sample_rate = sample_rate
        self.concatenate = concatenate
        self.static_file_path_name = static_file_path_name
        self.file_path = file_path
        self.archive_file_path = archive_file_path
        self.nmea_prepend = nmea_prepend
        self.nmea_append = nmea_append

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        ps.IngestFile.__init__(self)

        if self.nmea_prepend is None : self.nmea_prepend = ''
        if self.nmea_append is None : self.nmea_append = ''

        self.nmea = tr.NmeaSentence(prepend = self.nmea_prepend, append = self.nmea_append)


    def run(self) :

        while True :

            data_lines = []

            while not data_lines :
                #try :
                #    text_file = open(self.static_file_path_name, "r")
                #    data_lines = text_file.read().splitlines()
                #except OSError as e :
                #    rt.logging.exception(e)
                #rt.logging.debug("data_lines", data_lines)
                data_lines = ps.load_text_file_lines(self.static_file_path_name)
                time.sleep(1.0)

            char_data_lines = data_lines
            if self.concatenate is not None and self.concatenate :
                char_data_lines = [data_lines]
            rt.logging.debug("char_data_lines", char_data_lines)

            for char_data in char_data_lines :

                if self.concatenate is None or not self.concatenate :
                    char_data = [char_data]
                rt.logging.debug("char_data", char_data)

                timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate)
                nmea_data_array = self.nmea.decode_to_channels(char_data = char_data, channel_data = self.channels)
                rt.logging.debug("nmea_data_array", nmea_data_array)
                for nmea_data in nmea_data_array :
                    rt.logging.debug('nmea_data', nmea_data)
                    if nmea_data is not None :
                        (selected_tag, data_array), = nmea_data.items()
                        rt.logging.debug("selected_tag", selected_tag, "data_array", data_array)
                        self.write(data_array = data_array, selected_tag = selected_tag, timestamp_secs = timestamp_secs, timestamp_microsecs = timestamp_microsecs)

                time.sleep(1/self.sample_rate)



class CmemsFile(ap.NativeCmems, ps.IngestFile) :


    def __init__(self):

        ap.NativeCmems.__init__(self)
        ps.IngestFile.__init__(self)



class NativeCmemsNumpyFile(CmemsFile) :


    def __init__(self, channels = None, start_delay = None, sample_rate = None, product_id = None, service_id = None, service_user = None, service_pwd = None, out_file_name = None, file_path = None, archive_file_path = None, config_filepath = None, config_filename = None) :

        self.channels = channels
        self.start_delay = start_delay
        self.sample_rate = sample_rate
        self.product_id = product_id
        self.service_id = service_id
        self.service_user = service_user
        self.service_pwd = service_pwd
        self.out_file_name = out_file_name
        self.file_path = file_path
        self.archive_file_path = archive_file_path

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        CmemsFile.__init__(self)

        self.netcdf = tr.NetCdf()


    def run(self) :

        while True :

            self.send_request()

            complete_file_name = self.file_path + self.out_file_name
            netcdf_data = None
            while not netcdf_data :
                netcdf_data = NetCDFFile(complete_file_name)
                time.sleep(1.0)
            if netcdf_data :
                ps.delete_files([complete_file_name])
            rt.logging.debug("netcdf_data", netcdf_data)

            time_object = netcdf_data.variables["time"] #numpy.array(
            timestamps = time_object[:].data
            rt.logging.debug("timestamps", timestamps)
            latitude_object = netcdf_data.variables["lat"]
            latitudes = latitude_object[:].data
            rt.logging.debug("latitudes", latitudes)
            longitude_object = netcdf_data.variables["lon"]
            longitudes = longitude_object[:].data
            rt.logging.debug("longitudes", longitudes)
            all_values_time_lat_lon_object = netcdf_data.variables["VHM0"] #numpy.array(
            all_values_time_lat_lon = all_values_time_lat_lon_object[:,:,:].data
            rt.logging.debug("all_values_time_lat_lon", all_values_time_lat_lon)
            all_times_lats_lons_values_flat = []
            for timestamp, time_values in zip(timestamps, all_values_time_lat_lon) :
                new_times_lats_lons_values_flat = numpy.concatenate( ( [int(timestamp)], [len(latitudes)], [latitudes], [len(longitudes)], [longitudes], time_values ), axis = None )
                rt.logging.debug("new_times_lats_lons_values_flat", new_times_lats_lons_values_flat)
                all_times_lats_lons_values_flat = numpy.concatenate( ( all_times_lats_lons_values_flat, new_times_lats_lons_values_flat ) )
            rt.logging.debug("all_times_lats_lons_values_flat", all_times_lats_lons_values_flat, "len(all_times_lats_lons_values_flat)", len(all_times_lats_lons_values_flat))

            channel_indices, = self.channels['NETCDF'].items()
            data_array = [ { channel_indices[0] : all_times_lats_lons_values_flat } ]
            rt.logging.debug("data_array", data_array)
            #packed = msgpack_numpy.packb(unpacked, default = msgpack_numpy.encode)
            #print("packed", packed, len(packed), "len(packed)")
            #packed_ascii = base64.b64encode(packed)
            #print("packed_ascii", packed_ascii, len(packed_ascii), "len(packed_ascii)")
            #packed = base64.b64decode(packed_ascii)
            #unpacked = msgpack_numpy.unpackb(packed, object_hook = msgpack_numpy.decode) #strict_map_key=False)
            #values = unpacked[3]
            #print("unpacked[3]", unpacked[3], "len(unpacked[3])", len(unpacked[3]) )
            json_string = '['
            for timestamp, time_values in zip(timestamps[0:1], all_values_time_lat_lon[0:1]) :
                for lat, lon_values in zip(latitudes, time_values) :
                    for lon, value in zip(longitudes, lon_values) :
                        print("lat", lat, "lon", lon, "value", value)
                        json_string += '[null, null, "0", {"type": 1, "mmsi": "' + str('{0:.3g}'.format(value)) + '", "lat":' + str('{0:.7g}'.format(lat)) + ', "lon":' + str('{0:.7g}'.format(lon)) + '}], '
            json_string = json_string[:-2] + ']'
            print("json_string", json_string)
            data_array = [ { channel_indices[0] : json_string } ]

            timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate)
            self.write(data_array = data_array, selected_tag = 'NETCDF', timestamp_secs = timestamp_secs, timestamp_microsecs = timestamp_microsecs)

            time.sleep(1/self.sample_rate)



class SerialFile(ps.IngestFile, ps.LoadFile) :


    def __init__(self) :

        ps.LoadFile.__init__(self)
        ps.IngestFile.__init__(self) # IngestFile base class inherits from base class of LoadFile. Deadly Diamond of Death should not be a problem.


    def init_serial(self) :

        comport_list = serial.tools.list_ports.comports()
        for comport in comport_list:
            port_string = str(comport.device) + ';' + str(comport.name) + ';' + str(comport.description) + ';' + str(comport.hwid) + ';' + str(comport.vid) + ';' + str(comport.pid) + ';' + str(comport.serial_number) + ';' + str(comport.location) + ';' + str(comport.manufacturer) + ';' + str(comport.product) + ';' + str(comport.interface)
            rt.logging.debug("port_string", port_string)

            if self.port is None :
                rt.logging.debug("self.location", self.location)
                if (port_string.find(self.location)) >= 0:
                    self.port = comport.device
                    rt.logging.debug("self.port", self.port)

        self.serial_conn = None

        try :
            self.serial_conn = serial.Serial(port = self.port, baudrate = self.baudrate, timeout = self.timeout, parity = self.parity, stopbits = self.stopbits, bytesize = self.bytesize) #parity = serial.PARITY_EVEN, stopbits = serial.STOPBITS_ONE, bytesize = serial.SEVENBITS, write_timeout=1, , , , xonxoff=False, rtscts=False, dsrdtr=False)
            time.sleep(0.1)
            if (self.serial_conn.isOpen()):
                rt.logging.debug("connected to : " + self.serial_conn.portstr)
        except serial.serialutil.SerialException as e:
            rt.logging.exception(e)
        rt.logging.debug("self.serial_conn", self.serial_conn)


    def load_file(self, current_file = None):

        return ps.load_text_string_file(current_file)



class SerialNmeaFile(SerialFile) :


    def __init__(self, channels = None, ctrl_channels = None, port = None, start_delay = None, sample_rate = None, location = None, baudrate = None, timeout = None, parity = None, stopbits = None, bytesize = None, delay_waiting_check = None, file_path = None, ctrl_file_path = None, archive_file_path = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.ctrl_channels = ctrl_channels
        self.port = port
        self.start_delay = start_delay
        self.sample_rate = sample_rate
        self.location = location
        self.baudrate = baudrate
        self.timeout = timeout
        self.parity = parity
        self.stopbits = stopbits
        self.bytesize = bytesize
        self.delay_waiting_check = delay_waiting_check
        self.file_path = file_path
        self.ctrl_file_path = ctrl_file_path
        self.archive_file_path = archive_file_path

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        SerialFile.__init__(self)

        self.nmea = tr.NmeaSentence(prepend = '', append = '')


    def get_filenames(self, channel_data = None, file_path = None) :

        channel_list = []
        filetype_list = []

        rt.logging.debug("channel_data", channel_data)
        if channel_data is None :
            channel_array = []
        else :
            channel_array = [ tag_channels for channel_tag, tag_channels in channel_data.items() ]
            for channel, file_type in channel_array[0].items() :
                channel_list.append(channel)
                filetype_list.append(file_type)
        channels = set(channel_list)
        file_types = set(filetype_list)
        rt.logging.debug("channels", channels, "file_types", file_types)
        files = []
        for channel in channels :
            new_files = ps.get_all_files(path = file_path, extensions = file_types, channel = channel)
            rt.logging.debug("new_files", new_files)
            files.extend(new_files)

        return files


    def run(self) :

        while True:

            self.init_serial()

            data_string = ''

            while self.serial_conn.isOpen():

                files = self.get_filenames(channel_data = self.ctrl_channels, file_path = self.ctrl_file_path)
                rt.logging.debug('self.ctrl_channels', self.ctrl_channels)
                rt.logging.debug('files', files)

                for current_file in files :
                    rt.logging.debug("current_file", current_file)
                    self.retrieve_file_data(current_file)

                    #print("self.acquired_bytes", self.acquired_bytes)
                    #self.serial_conn.write(self.acquired_bytes)
                    #self.serial_conn.write(b'$GPGGA,121328.00,6237.6000000,N,01757.4000000,E,1,9,0.91,44.7,M,24.4,M,,*55\r\n')
                    try :
                        os.remove(self.current_file)
                    except (PermissionError, FileNotFoundError) as e :
                        rt.logging.exception(e)
                    #timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate)
                    #nmea_data_array = self.nmea.decode_to_channels(char_data = self.acquire_base64, channel_data = self.ctrl_channels, time_tuple = current_timetuple, line_end = ' ')
                    #from_time_pos(timestamp, latitude, longitude)
                #self.serial_conn.write(b'$GPGGA,204000.000,6237.8000,N,01757.0000,E,1,9,0.91,44.7,M,24.4,M,,*62\r\n')

                response = ''
                while self.serial_conn.in_waiting:
                    response = self.serial_conn.read()
                    response_string = ''
                    try:
                        response_string = response.decode()
                    except UnicodeDecodeError as e:
                        rt.logging.exception(e)
                    data_string += response_string

                    time.sleep(self.delay_waiting_check)

                if data_string != '' :

                    rt.logging.debug("data_string", data_string)
                    #self.ais_test_monitor(data_string)

                    data_lines = data_string.splitlines()
                    rt.logging.debug("data_lines", data_lines)

                    timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate)
                    nmea_data_array = self.nmea.decode_to_channels(char_data = data_lines, channel_data = self.channels, time_tuple = current_timetuple, line_end = ' ')

                    for nmea_data in nmea_data_array :
                        if nmea_data is not None :
                            (selected_tag, data_array), = nmea_data.items()
                            self.write(data_array = data_array, selected_tag = selected_tag, timestamp_secs = timestamp_secs, timestamp_microsecs = timestamp_microsecs)

                data_string = ''

                time.sleep(1/self.sample_rate)

            self.serial_conn.close()

            time.sleep(5)


    def ais_test_monitor(self, nmea_stream) :

        nmea_strings = nmea_stream.splitlines()
        data_string_temp_file = tempfile.NamedTemporaryFile(mode = 'w+', delete = False)
        for nmea_string in nmea_strings :
            data_string_temp_file.write(nmea_string + '\n')
        data_string_temp_file.close()
        pyais_stream = pyais.stream.FileReaderStream(data_string_temp_file.name)
        try :
            for ais_message in pyais_stream :
                ais_data = {}
                try :
                    ais_data = ais_message.decode().content
                except (ValueError, IndexError, pyais.exceptions.InvalidNMEAMessageException, pyais.exceptions.InvalidChecksumException) as e :
                    rt.logging.exception(e)
                if ut.safe_get(ais_data, 'mmsi', '') != '265811800' : #ut.safe_get(ais_data, 'type', 0) == 8 :
                    rt.logging.debug('ais_message', ais_message)
                    rt.logging.debug('ais_data', ais_data)
                    rt.logging.debug(' ')
        except (TypeError, ValueError, pyais.exceptions.InvalidNMEAMessageException) as e :
            rt.logging.exception(e)



class UdpFile(ap.UdpReceive, ps.IngestFile):


    def __init__(self):

        ap.UdpReceive.__init__(self)
        ps.IngestFile.__init__(self)



class NmeaUdpFile(UdpFile):


    def __init__(self, channels = None, ip_list = None, port = None, start_delay = None, sample_rate = None, transmit_rate = None, file_path = None, archive_file_path = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.ip_list = ip_list
        self.port = port
        self.start_delay = start_delay
        self.sample_rate = sample_rate
        self.transmit_rate = transmit_rate
        self.file_path = file_path
        self.archive_file_path = archive_file_path

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        UdpFile.__init__(self)

        self.nmea = tr.NmeaSentence(prepend = '', append = '')


    def run(self):

        time.sleep(self.start_delay)

        while True :

            #time.sleep(1/self.transmit_rate)

            data = None
            address = None

            data, address = self.socket.recvfrom(4096)
            rt.logging.debug("address", address, "data", data)

            data_lines = []
            if data is not None :
                data_lines = data.decode("utf-8").splitlines()
                rt.logging.debug("data_lines", data_lines)

            timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate)
            nmea_data_array = self.nmea.decode_to_channels(char_data = data_lines, channel_data = self.channels, time_tuple = current_timetuple, line_end = None)
            rt.logging.debug("nmea_data_array", nmea_data_array)
            for nmea_data in nmea_data_array :
                if nmea_data is not None :
                    (selected_tag, data_array), = nmea_data.items()
                    rt.logging.debug("data_array", data_array)
                    if not ( None in data_array ) :
                        self.write(data_array = data_array, selected_tag = selected_tag, timestamp_secs = timestamp_secs, timestamp_microsecs = timestamp_microsecs)



class RawUdpFile(UdpFile):


    def __init__(self, channels = None, ip_list = None, port = None, start_delay = None, sample_rate = None, transmit_rate = None, file_path = None, archive_file_path = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.ip_list = ip_list
        self.port = port
        self.start_delay = start_delay
        self.sample_rate = sample_rate
        self.transmit_rate = transmit_rate
        self.file_path = file_path
        self.archive_file_path = archive_file_path

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        UdpFile.__init__(self)

        self.nmea = tr.NmeaSentence()


    def run(self):

        time.sleep(self.start_delay)
        selected_tag = 'DUMMY'
        self.channels = {selected_tag: self.channels}

        while True :

            #time.sleep(1/self.transmit_rate)

            data = None
            address = None

            data, address = self.socket.recvfrom(4096)
            rt.logging.debug("address", address, "data", data)
            values = struct.unpack('>HIf', data)
            rt.logging.debug("values", values)
            data_array =  [ { values[0]:[values[2]] } ]
            rt.logging.debug("data_array", data_array)
            rt.logging.debug("self.channels", self.channels)
            self.write(data_array = data_array, selected_tag = selected_tag, timestamp_secs = values[1])



class HttpFile(ap.HttpClient, ps.IngestFile) :


    def __init__(self):

        ap.HttpClient.__init__(self)
        ps.IngestFile.__init__(self)



class HttpAishubAivdmFile(HttpFile) :


    def __init__(self, channels = None, ip_list = None, http_scheme = None, start_delay = None, sample_rate = None, transmit_rate = None, client_api_url = None, max_connect_attempts = None, file_path = None, archive_file_path = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.ip_list = ip_list
        self.http_scheme = http_scheme
        self.start_delay = start_delay
        self.sample_rate = sample_rate
        self.transmit_rate = transmit_rate
        self.client_api_url = client_api_url
        self.max_connect_attempts = max_connect_attempts
        self.file_path = file_path
        self.archive_file_path = archive_file_path

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        HttpFile.__init__(self)

        self.ais = tr.Ais()


    def run(self) :

        time.sleep(self.start_delay)

        #channel = list(self.channels.keys())[0]
        #selected_tag = 'AISHUB'
        #self.channels = {selected_tag: self.channels}

        while True :

            result = self.get_external()
            data_lines = result.text
            rt.logging.debug("data_lines", data_lines)
            first_line_break_index = data_lines.find('\n')
            data_lines_list_no_header = []
            if first_line_break_index > 0 :
                data_lines_list_no_header.append( data_lines[first_line_break_index + 1 : ] )
            rt.logging.debug("data_lines_list_no_header", data_lines_list_no_header)

            for char_data in data_lines_list_no_header :

                rt.logging.debug("char_data", char_data)
                char_data_lines = char_data.splitlines()[1:]
                rt.logging.debug("char_data_lines", char_data_lines)
                key_list = key_list = ['mmsi', 'timestamp']
                timestamps = [ tr.dict_from_csv(key_list, char_data_line, ',')['timestamp'] for char_data_line in char_data_lines ]
                rt.logging.debug("timestamps", timestamps)
                unique_timestamps = list(set(timestamps))
                rt.logging.debug("unique_timestamps", unique_timestamps)
                unique_timestamps_dict = tr.dict_from_lists(unique_timestamps, [ ['AISHUB\n'] for _ in unique_timestamps ])
                rt.logging.debug("unique_timestamps_dict", unique_timestamps_dict)
                for timestamp_secs, char_data_line in zip(timestamps, char_data_lines) :
                    unique_timestamps_dict[timestamp_secs][0] += char_data_line + '\n'
                    #print(timestamp, ut.safe_index(timestamps, timestamp))
                rt.logging.debug("unique_timestamps_dict", unique_timestamps_dict)

                #tagged_char_data = ['AISHUB\n' + char_data]
                #print("tagged_char_data", tagged_char_data)

                for (timestamp_secs, timestamped_char_data) in unique_timestamps_dict.items() :

                    rt.logging.debug("timestamped_char_data", timestamped_char_data)

                    ais_data_array = self.ais.decode_to_channels(char_data = timestamped_char_data, channel_data = self.channels)

                    rt.logging.debug("ais_data_array", ais_data_array)
                    #timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate)
                    for ais_data in ais_data_array :
                        rt.logging.debug('ais_data', ais_data)
                        if ais_data is not None :
                            (selected_tag, data_array), = ais_data.items()
                            rt.logging.debug("selected_tag", selected_tag, "data_array", data_array)
                            self.write(data_array = data_array, selected_tag = selected_tag, timestamp_secs = int(timestamp_secs)) #, timestamp_microsecs = timestamp_microsecs)

#                time.sleep(1/self.sample_rate)
#            data_array =  [ { channel: result_text} ]
#            rt.logging.debug("data_array", data_array)
#            timestamp_secs, current_timetuple, timestamp_microsecs, next_sample_secs = tr.timestamp_to_date_times(sample_rate = self.sample_rate)
#            self.write(data_array = data_array, selected_tag = selected_tag, timestamp_secs = timestamp_secs)

            time.sleep(1/self.sample_rate)




class Image(ps.IngestFile) :


    def __init__(self) :

        self.env = self.get_env()
        if self.video_res is None: self.video_res = self.env['VIDEO_RES']
        if self.video_quality is None: self.video_quality = self.env['VIDEO_QUALITY']
        (self.channel,) = self.channels
        self.capture_filename = 'image_' + str(self.channel) + '.' + self.file_extension
        rt.logging.debug("self.capture_filename", self.capture_filename)
        ps.IngestFile.__init__(self)


    def run(self):

        time.sleep(self.start_delay)

        divisor = numpy.int64(1/numpy.float64(self.sample_rate))
        current_time = numpy.float64(time.time())
        current_secs = numpy.int64(current_time)

        while True :

            next_sample_secs = current_secs + numpy.int64( divisor - current_secs % divisor )
            current_time = numpy.float64(time.time())
            current_secs = numpy.int64(current_time)
            if next_sample_secs > current_secs :
                time.sleep(0.1)
            else :

                self.read_samples()

                if self.file_path is not None and os.path.exists(self.file_path):
                    try:
                        capture_file_timestamp = int(os.path.getmtime(self.capture_filename))
                        store_filename = self.file_path + str(self.channel) + '_' + str(capture_file_timestamp) + '.' + self.file_extension
                        archive_filename = self.archive_file_path + str(self.channel) + '_' + str(capture_file_timestamp) + '.' + self.file_extension
                        shutil.copy(self.capture_filename, store_filename)
                        if self.archive_file_path is not None and os.path.exists(self.archive_file_path):
                            pass
                            #shutil.copy(capture_filename, archive_filename)
                    except (FileNotFoundError, PermissionError) as e:
                        rt.logging.exception(e)



class USBCam(Image):


    def __init__(self, channels = None, start_delay = 0, sample_rate = None, file_path = None, archive_file_path = None, file_extension = 'jpg', video_unit = None, video_res = None, video_rate = None, video_quality = None, video_capture_method = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.sample_rate = sample_rate

        self.file_path = file_path
        self.archive_file_path = archive_file_path
        self.file_extension = file_extension
        self.video_unit = video_unit
        self.video_res = video_res
        self.video_rate = video_rate
        self.video_quality = video_quality
        self.video_capture_method = video_capture_method

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        self.env = self.get_env()
        if self.video_unit is None: self.video_unit = self.env['VIDEO_UNIT']
        if self.video_rate is None: self.video_rate = self.env['VIDEO_RATE']
        if self.video_capture_method is None: self.video_capture_method = self.env['VIDEO_CAPTURE_METHOD']

        Image.__init__(self)

        if str.lower(self.video_capture_method) == 'opencv' :

            self.cam = cv2.VideoCapture( int(''.join(filter(str.isdigit, self.video_unit))), cv2.CAP_ANY )  # cv2.CAP_OPENCV_MJPEG
            resolutions = [ (320,200), (320,240), (640,480), (720,480), (854,450), (800,480), (768,576), (800,600), (1024,768), (1152,768), (1280,720), (1280,800), (1280,768), (1280,1024), (1366,768), (1440,960), (1400,1050), (1680,1050), (1600,1200), (1920,1080), (1920,1200) ]
            for res in resolutions :
                self.cam.set(cv2.CAP_PROP_FRAME_WIDTH , res[0])
                self.cam.set(cv2.CAP_PROP_FRAME_HEIGHT, res[1])
                width = self.cam.get(cv2.CAP_PROP_FRAME_WIDTH)
                height = self.cam.get(cv2.CAP_PROP_FRAME_HEIGHT)
                if width == res[0] and height == res[1] :
                    rt.logging.debug("width, height: ", width, height)
            huge_dim = 10000
            self.cam.set(cv2.CAP_PROP_FRAME_WIDTH , huge_dim)
            self.cam.set(cv2.CAP_PROP_FRAME_HEIGHT, huge_dim)
            max_width = self.cam.get(cv2.CAP_PROP_FRAME_WIDTH)
            max_height = self.cam.get(cv2.CAP_PROP_FRAME_HEIGHT)
            self.cam.set(cv2.CAP_PROP_FRAME_WIDTH , max_width)
            self.cam.set(cv2.CAP_PROP_FRAME_HEIGHT, max_height)
            self.cam.set(cv2.CAP_PROP_FPS , self.video_rate)

        if str.lower(self.video_capture_method) == 'raspicam' :

            import picamera

            self.picam = picamera.PiCamera()
            self.picam.resolution = (1280, 720)
            self.picam.hflip = False
            self.picam.vflip = False
            #self.picam.rotation = 90
            self.picam.brightness = 50
            #self.picam.zoom = (0.6, 0.6, 0.5, 0.5)
            #self.picam.awb_mode = 'off'
            #self.picam.awb_gains = (1.0, 2.5)
            self.picam.shutter_speed = 50000
            self.picam.exposure_compensation = -24
            self.picam.iso = 100
            self.picam.contrast = -15

    def read_samples(self, sample_secs = -9999):

        try :

            time_before = time.time()

            if str.lower(self.video_capture_method) == 'opencv' :
                rt.logging.debug("self.capture_filename", self.capture_filename, "...")
                ret, frame = self.cam.read()
                if not ret : rt.logging.debug("...read failure!")
                if ret :
                    frame = cv2.resize(frame, tuple(self.video_res), interpolation = cv2.INTER_AREA)
                    cv2.imwrite( self.capture_filename, frame, [cv2.IMWRITE_JPEG_QUALITY, self.video_quality] )

            elif str.lower(self.video_capture_method) == 'uvccapture' :
                os.system('uvccapture -m -x' + str(self.video_res[0]) + ' -y' + str(self.video_res[1]) + ' -q' + str(self.video_quality) + ' -d' + self.video_unit + ' -o' + self.capture_filename)

            elif str.lower(self.video_capture_method) == 'ffmpeg' :
                ffmpeg_string = 'ffmpeg -y -f v4l2 -hide_banner -loglevel warning -i ' + self.video_unit + ' -s ' + str(self.video_res[0]) + 'x' + str(self.video_res[1]) + ' -vframes 1 ' + self.capture_filename
                rt.logging.debug("ffmpeg_string", ffmpeg_string)
                os.system(ffmpeg_string)  # -input_format mjpeg

            elif str.lower(self.video_capture_method) == 'raspicam' :
                self.picam.capture(self.capture_filename, format='jpeg', quality=10)

            else : # fswebcam
                fswebcam_string = 'fswebcam -q -d ' + self.video_unit + ' -r ' + str(self.video_res[0]) + 'x' + str(self.video_res[1]) + ' --fps ' + str(self.video_rate) + ' -S 2 --jpeg ' + str(self.video_quality) + ' --no-banner --save ' + self.capture_filename
                rt.logging.debug("fswebcam_string", fswebcam_string)
                os.system(fswebcam_string)
                # --set "Brightness"=127 --set "Contrast"=63 --set "Saturation"=127 --set "Hue"=90 --set "Gamma"=250 --set "White Balance Temperature, Auto"=True

            rt.logging.debug("Capture time: ", time.time() - time_before)

        except PermissionError as e :

            rt.logging.exception(e)



class ScreenshotUpload(Image):


    def __init__(self, channels = None, ip_list = None, http_scheme = None, sample_rate = None, host_api_url = None, client_api_url = None, crop = None, video_quality = None, config_filepath = None, config_filename = None):

        self.channels = channels

        self.sample_rate = sample_rate
        self.host_api_url = host_api_url
        self.client_api_url = client_api_url
        self.crop = crop
        self.video_quality = video_quality

        self.start_delay = 0
        self.max_connect_attempts = 50
        self.file_extension = 'jpg'

        self.file_path = None
        self.archive_file_path = None
        self.video_res = None

        self.ip_list = ip_list
        self.http_scheme = http_scheme

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        self.env = self.get_env()
        if self.ip_list is None: self.ip_list = self.env['IP_LIST']
        if self.http_scheme is None: self.http_scheme = self.env['HTTP_SCHEME']

        Image.__init__(self)


    def read_samples(self, sample_secs = -9999):

        try :

            img = ImageGrab.grab( bbox = (self.crop[0], self.crop[1], self.crop[2], self.crop[3]) )
            jpeg_image_buffer = io.BytesIO()
            img.save(jpeg_image_buffer, format="JPEG")
            img_str = base64.b64encode(jpeg_image_buffer.getvalue())
            http = li.DirectUpload(channels = self.channels, start_delay = self.start_delay, host_api_url = self.host_api_url, client_api_url = self.client_api_url, max_connect_attempts = self.max_connect_attempts, http_scheme = self.http_scheme)
            (channel,) = self.channels

            for current_ip in self.ip_list :
                res = http.send_request(start_time = sample_secs, end_time = sample_secs, duration = 10, unit = 1, delete_horizon = 3600, ip = current_ip)
                data_string = str(channel) + ';' + str(sample_secs) + ',-9999.0,,' + str(img_str.decode('utf-8')) + ',;'
                rt.logging.debug('data_string', data_string[:100])
                res = http.set_requested(data_string, ip = current_ip)

        except PermissionError as e :
            rt.logging.exception(e)


    def run(self):

        time.sleep(self.start_delay)

        divisor = numpy.int64(1/numpy.float64(self.sample_rate))
        current_time = numpy.float64(time.time())
        current_secs = numpy.int64(current_time)

        while True :

            next_sample_secs = current_secs + numpy.int64( divisor - current_secs % divisor )
            current_time = numpy.float64(time.time())
            current_secs = numpy.int64(current_time)
            if next_sample_secs > current_secs :
                time.sleep(0.1)
            else :
                self.read_samples(next_sample_secs)



class TempFileUpload(Image):


    def __init__(self, channels = None, ip_list = None, http_scheme = None, sample_rate = None, host_api_url = None, client_api_url = None, file_extension = 'jpg', config_filepath = None, config_filename = None):

        self.channels = channels

        self.sample_rate = sample_rate
        self.host_api_url = host_api_url
        self.client_api_url = client_api_url

        self.start_delay = 0
        self.max_connect_attempts = 50
        self.file_extension = file_extension

        self.file_path = None
        self.archive_file_path = None

        self.video_res = None
        self.video_quality = None

        self.ip_list = ip_list
        self.http_scheme = http_scheme

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        self.env = self.get_env()
        if self.ip_list is None: self.ip_list = self.env['IP_LIST']
        if self.http_scheme is None: self.http_scheme = self.env['HTTP_SCHEME']

        Image.__init__(self)


    def upload_file(self, sample_secs = -9999):

        try :
            rt.logging.debug("self.capture_filename", self.capture_filename)
            with open(self.capture_filename, "rb") as image_file:
                img_str = base64.b64encode(image_file.read())
            http = li.DirectUpload(channels = self.channels, start_delay = self.start_delay, host_api_url = self.host_api_url, client_api_url = self.client_api_url, max_connect_attempts = self.max_connect_attempts, http_scheme = self.http_scheme)
            (channel,) = self.channels

            for current_ip in self.ip_list :
                res = http.send_request(start_time = sample_secs, end_time = sample_secs, duration = 10, unit = 1, delete_horizon = 3600, ip = current_ip)
                data_string = str(channel) + ';' + str(sample_secs) + ',-9999.0,,' + str(img_str.decode('utf-8')) + ',;'
                rt.logging.debug('data_string', data_string[:100])
                res = http.set_requested(data_string, ip = current_ip)

        except PermissionError as e :
            rt.logging.exception(e)


    def run(self):

        time.sleep(self.start_delay)

        divisor = numpy.int64(1/numpy.float64(self.sample_rate))
        current_time = numpy.float64(time.time())
        current_secs = numpy.int64(current_time)

        while True :

            next_sample_secs = current_secs + numpy.int64( divisor - current_secs % divisor )
            current_time = numpy.float64(time.time())
            current_secs = numpy.int64(current_time)
            if next_sample_secs > current_secs :
                time.sleep(0.1)
            else :
                self.upload_file(next_sample_secs)



class NidaqVoltageIn(ps.IngestFile):


    def __init__(self, sample_rate = 1, samplesPerChan = 1, subSamplesPerChan = 1, minValue = 0, maxValue = 10, IPNumber = "", moduleSlotNumber = 1, moduleChanRange = [0], uniqueChanIndexRange = [0]):

        self.nidaq = dv.NidaqVoltageIn(sample_rate, samplesPerChan, subSamplesPerChan, minValue, maxValue, IPNumber, moduleSlotNumber, moduleChanRange, uniqueChanIndexRange)

        ps.IngestFile.__init__(self)


    def run(self):

        self.nidaq.InitAcquire()
        while True:
            if self.nidaq.globalErrorCode >= 0:
                self.nidaq.LoopAcquire()
                if self.nidaq.globalErrorCode < 0:
                    self.nidaq.StopAndClearTasks()
                    self.nidaq.InitAcquire()
            else:
                self.nidaq.StopAndClearTasks()
                self.nidaq.InitAcquire()
            time.sleep(10)



class NidaqCurrentIn(ps.IngestFile):


    def __init__(self, sample_rate = 1, samplesPerChan = 1, subSamplesPerChan = 1, minValue = 0, maxValue = 10, IPNumber = "", moduleSlotNumber = 1, moduleChanRange = [0], uniqueChanIndexRange = [0]):

        self.nidaq = daqc.device.NidaqVoltageIn(sample_rate, samplesPerChan, subSamplesPerChan, minValue, maxValue, IPNumber, moduleSlotNumber, moduleChanRange, uniqueChanIndexRange)

        ps.IngestFile.__init__(self)


    def run(self):

        self.nidaq.InitAcquire()
        while True:
            if self.nidaq.globalErrorCode >= 0:
                self.nidaq.LoopAcquire()
                if self.nidaq.globalErrorCode < 0:
                    self.nidaq.StopAndClearTasks()
                    self.nidaq.InitAcquire()
            else:
                self.nidaq.StopAndClearTasks()
                self.nidaq.InitAcquire()
            time.sleep(10)



class AcquireCurrent(ps.IngestFile):

    """ Adapted from https://scipy-cookbook.readthedocs.io/items/Data_Acquisition_with_NIDAQmx.html."""


    def __init__(self, channels = None, start_delay = 0, sample_rate = None, file_path = None, archive_file_path = None, config_filepath = None, config_filename = None):

        self.channels = channels
        self.start_delay = start_delay
        self.sample_rate = sample_rate

        self.file_path = file_path
        self.archive_file_path = archive_file_path

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        ps.IngestFile.__init__(self)

        self.nidaq = None
        if sys.platform.startswith('win32') :
            self.nidaq = ctypes.windll.nicaiu

        self.uInt8 = ctypes.c_ubyte
        self.int32 = ctypes.c_long
        self.uInt32 = ctypes.c_ulong
        self.uInt64 = ctypes.c_ulonglong
        self.float64 = ctypes.c_double
        self.bool32 = ctypes.c_bool
        self.TaskHandle = self.uInt32
        self.pointsWritten = self.uInt32()
        self.pointsRead = self.uInt32()
        self.null = ctypes.POINTER(ctypes.c_int)()
        self.value = self.uInt32()

        self.DAQmx_Val_Cfg_Default = self.int32(-1)
        self.DAQmx_Val_Auto = self.int32(-1)
        self.DAQmx_Val_Internal = self.int32(10200)
        self.DAQmx_Val_Volts = self.int32(10348)
        self.DAQmx_Val_Rising = self.int32(10280)
        self.DAQmx_Val_Falling = self.int32(10171)
        self.DAQmx_Val_CountUp = self.int32(10128)
        self.DAQmx_Val_FiniteSamps = self.int32(10178)
        self.DAQmx_Val_GroupByChannel = self.int32(0)
        self.DAQmx_Val_ChanForAllLines = self.int32(1)
        self.DAQmx_Val_RSE = self.int32(10083)
        self.DAQmx_Val_Diff = self.int32(10106)
        self.DAQmx_Val_Amps = self.int32(10342)
        self.DAQmx_Val_ContSamps = self.int32(10123)
        self.DAQmx_Val_GroupByScanNumber = self.int32(1)
        self.DAQmx_Val_Task_Reserve = self.int32(4)
        self.DAQmx_Val_ChanPerLine = self.int32(0)

        self.SAMPLE_RATE = 1000
        self.SAMPLES_PER_CHAN = 1000

        self.taskCurrent = self.TaskHandle(0)

        self.minCurrent = self.float64(-0.02)
        self.maxCurrent = self.float64(0.02)
        self.bufferSize = self.uInt32(self.SAMPLES_PER_CHAN)
        self.pointsToRead = self.bufferSize
        self.pointsRead = self.uInt32()
        self.sampleRate = self.float64(self.SAMPLE_RATE)
        self.samplesPerChan = self.uInt64(self.SAMPLES_PER_CHAN)
        self.clockSource = ctypes.create_string_buffer(b"OnboardClock")
        self.IPnumber2 = ctypes.create_string_buffer(b"169.254.254.253")
        self.defaultModuleName22 = ctypes.create_string_buffer(b"cDAQ9188-1AD0C2FMod2")
        self.timeout = self.float64(100.0)

        self.device2NameOut = ctypes.create_string_buffer(100)
        self.device2NameOutBufferSize = self.uInt32(100)
        self.device2ModuleNamesOut = ctypes.create_string_buffer(1000)
        self.device2ModuleNamesOutBufferSize = self.uInt32(1000)
        self.module21ChansOut = ctypes.create_string_buffer(2000)
        self.module21ChansOutBufferSize = self.uInt32(2000)
        self.module22ChansOut = ctypes.create_string_buffer(2000)
        self.module22ChansOutBufferSize = self.uInt32(2000)

        self.data = numpy.zeros((1000,),dtype=numpy.float64)

        self.global_error_code = 0


    def CHK(self, err):
        """a simple error checking routine"""
        global global_error_code
        global_error_code = err
        if err < 0:
            buf_size = 1000
            buf = ctypes.create_string_buffer(b"\000" * buf_size)
            self.nidaq.DAQmxGetErrorString(err,ctypes.byref(buf),buf_size)
            #raise RuntimeError('nidaq call failed with error %d: %s'%(err,repr(buf.value)))
            rt.logging.debug('nidaq call failed with error %d: %s'%(err,repr(buf.value)))


    def scale_cond_transmitter(self, I_cond):
        #return I_cond
        return ( I_cond * 1000.0 - 4.0 ) / 16.0 * 10e-6

    def scale_Opto_22_AD3_SATT_ETT45_0101(self, I_temp):
        return ( I_temp * 1000.0 - 4.0 ) / 16.0 * 50.0

    # Create Task and Voltage Channel and Configure Sample Clock
    def SetupTasks(self):
        self.device2NameOut = b"cDAQ9188-1AD0C2F"

        self.CHK( self.nidaq.DAQmxGetDevChassisModuleDevNames(self.device2NameOut, self.device2ModuleNamesOut, self.device2ModuleNamesOutBufferSize) )
        rt.logging.debug("device2ModuleNamesOut: ", repr(self.device2ModuleNamesOut.value))
        self.CHK( self.nidaq.DAQmxGetDevAIPhysicalChans(self.defaultModuleName22, self.module22ChansOut, 2000) )
        rt.logging.debug("module22ChansOut: ", repr(self.module22ChansOut.value))
        self.CHK(self.nidaq.DAQmxCreateTask("",ctypes.byref(self.taskCurrent)))
        self.CHK(self.nidaq.DAQmxCreateAICurrentChan(self.taskCurrent,self.module22ChansOut,"",self.DAQmx_Val_RSE,self.minCurrent,self.maxCurrent,self.DAQmx_Val_Amps,self.DAQmx_Val_Internal,None,None))
        self.CHK(self.nidaq.DAQmxCfgSampClkTiming(self.taskCurrent, self.clockSource, self.sampleRate, self.DAQmx_Val_Rising, self.DAQmx_Val_ContSamps, self.samplesPerChan))
        #CHK(nidaq.DAQmxCfgInputBuffer(taskCurrent,200000))


    def ReserveTasks(self):
        self.CHK(self.nidaq.DAQmxTaskControl(self.taskCurrent, self.DAQmx_Val_Task_Reserve))

    def StartTasks(self):
        self.CHK(self.nidaq.DAQmxStartTask(self.taskCurrent))

    def ReadCurrent(self):
        self.pointsToRead = self.bufferSize
        self.data = numpy.zeros((16*self.bufferSize.value,),dtype=numpy.float64)
        if global_error_code >= 0:
            self.CHK(self.nidaq.DAQmxReadAnalogF64(self.taskCurrent,self.pointsToRead,self.timeout,self.DAQmx_Val_GroupByChannel,self.data.ctypes.data,self.uInt32(16*self.bufferSize.value),ctypes.byref(self.pointsRead),None))
        return self.data

    def StopAndClearTasks(self):
        if self.taskCurrent.value != 0:
            self.CHK( self.nidaq.DAQmxStopTask(self.taskCurrent) )
            self.CHK( self.nidaq.DAQmxClearTask(self.taskCurrent) )

    def InitAcquire(self):
        self.SetupTasks()
        self.ReserveTasks()
        self.StartTasks()


    def LoopAcquire(self):

        while (global_error_code >= 0):

            current = 0.0
            try:
                current = self.ReadCurrent()
            except OSError as e:
                rt.logging.exception(e)

            if self.file_path is not None and os.path.exists(self.file_path):

                acq_finish_time = numpy.float64(time.time())
                acq_finish_secs = numpy.int64(acq_finish_time)
                acq_finish_microsecs = numpy.int64(acq_finish_time * 1e6)
                acq_microsec_part = acq_finish_microsecs - numpy.int64(acq_finish_secs)*1e6
                if acq_microsec_part > 990000 :
                    time.sleep(0.03)
                if acq_microsec_part < 10000 :
                    time.sleep(0.87)

                for channel_index in range(0, 16):
                    current_array = current[self.SAMPLES_PER_CHAN*(channel_index+0):self.SAMPLES_PER_CHAN*(channel_index+1)]
                    if channel_index == 0 : current_array = self.scale_cond_transmitter(current_array)
                    if channel_index == 1 : current_array = self.scale_Opto_22_AD3_SATT_ETT45_0101(current_array)
                    current_avg = tr.downsample(current_array, 1)
                    current_array = numpy.concatenate(([0.0], acq_microsec_part, current_array), axis = None)
                    current_array[0] = current_avg
                    try:
                        filename_current = repr(97+channel_index) + "_" + repr(acq_finish_secs)
                        numpy.save(self.file_path+filename_current, current_array)
                    except PermissionError as e:
                        rt.logging.exception(e)


    def run(self):

        self.InitAcquire()
        while True:
            if global_error_code >= 0:
                self.LoopAcquire()
                if global_error_code < 0:
                    self.StopAndClearTasks()
                    self.InitAcquire()
            else:
                self.StopAndClearTasks()
                self.InitAcquire()
            time.sleep(10)



class AcquireMaster:


    def __init__(self):
        pass


class AcquireVoltage:

    def __init__(self):
        pass
