import socket
import requests
import http
import time
import datetime
from optparse import OptionParser

try : from motu_utils import motu_api
except ImportError: pass

import gateway.runtime as rt
import gateway.task as ta
import gateway.transform as tr



class Native(ta.AcquireControlTask) :


    def __init__(self) :

        ta.AcquireControlTask.__init__(self)



class Udp(ta.IpTask):


    def __init__(self) :

        ta.IpTask.__init__(self)

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)



class UdpReceive(Udp):


    def __init__(self):

        Udp.__init__(self)

        server_address = ('', self.port)
        self.socket.bind(server_address)



class UdpSend(Udp):


    def __init__(self):

        Udp.__init__(self)


    def dispatch_packet(self, data_bytes = '', ip = '127.0.0.1'):

        try :
            self.socket.sendto(data_bytes, (ip, self.port))
        except OSError as e :
            rt.logging.exception(e)



class Http(ta.HttpTask):


    def __init__(self) :

        self.env = self.get_env()
        if self.max_connect_attempts is None: self.max_connect_attempts = self.env['MAX_CONNECT_ATTEMPTS']

        ta.HttpTask.__init__(self)

        self.connect_attempts = 0



class HttpMaint(ta.MaintenanceTask):


    def __init__(self) :

        self.env = self.get_env()
        if self.maint_api_url is None: self.maint_api_url = self.env['MAINT_API_URL']
        if self.http_scheme is None: self.http_scheme = self.env['HTTP_SCHEME']

        ta.MaintenanceTask.__init__(self)


    def partition_database(self, ip = '127.0.0.1'):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            complete_url = self.http_scheme + "://" + ip + self.maint_api_url + "partition_database.php"
            raw_data = requests.post(complete_url, timeout = 5, data = {"new_partition_name_date": self.new_partition_name_date, "new_partition_timestamp": self.new_partition_timestamp, "oldest_kept_partition_name_date": self.oldest_kept_partition_name_date})
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.partition_database(ip)
            else:
                exit(-1)


    def get_db_rows(self, ip = '127.0.0.1'):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            raw_data = requests.post(self.http_scheme + "://" + ip + self.maint_api_url + "get_db_rows.php", timeout = 5, data = {"table_label": self.table_label, "id_range": self.id_range})
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.get_db_rows(ip)
            else:
                exit(-1)



class HttpExternal(Http):


    def __init__(self) :

        Http.__init__(self)


    def send_request(self, ip = '127.0.0.1'):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            raw_data = requests.get(self.http_scheme + "://" + ip + self.external_api_url, timeout = 5)
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.send_request(ip)
            else:
                exit(-1)
        # https://data.aishub.net/ws.php?username=AH_3265_FB8EB51C&format=0&output=csv&compress=0&latmin=59.717918&latmax=66.396283&lonmin=16.595076&lonmax=27.064446


class HttpHost(Http):


    def __init__(self) :

        self.env = self.get_env()
        if self.host_api_url is None: self.host_api_url = self.env['HOST_API_URL']

        Http.__init__(self)


    def get_requested(self, ip = '127.0.0.1'):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            complete_url = self.http_scheme + "://" + ip + self.host_api_url + "get_requested.php"
            rt.logging.debug("complete_url", complete_url)
            raw_data = requests.post(complete_url, timeout = 5, data = {"channelrange": tr.get_channel_range_string(self.channels), "duration": 10, "unit": 1})
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.get_requested(ip)
            else:
                exit(-1)


    def set_requested(self, data_string, ip = '127.0.0.1'):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            rt.logging.debug("data_string", data_string)
            complete_url = self.http_scheme + "://" + ip + self.host_api_url + "set_requested.php"
            rt.logging.debug("complete_url", complete_url)
            raw_data = requests.post(complete_url, timeout = 5, data = {"returnstring": data_string})
            rt.logging.debug("raw_data", raw_data)
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.set_requested(data_string, ip)
            else:
                exit(-1)


    def clear_data_requests(self, ip = '127.0.0.1'):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            complete_url = self.http_scheme + "://" + ip + self.host_api_url + "clear_data_requests.php"
            rt.logging.debug("complete_url", complete_url)
            raw_data = requests.post(complete_url, timeout = 5, data = {"channelrange": tr.get_channel_range_string(self.channels)})
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.clear_data_requests(ip)
            else:
                exit(-1)


    def update_devices(self, ip = '127.0.0.1', host_id = 0, hardware_id = None, description = None):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            raw_data = requests.post(self.http_scheme + "://" + ip + self.host_api_url + "update_devices.php", timeout = 5, data = {"host_id": host_id, "hardware_id": hardware_id, "description": description})
            rt.logging.debug("raw_data", raw_data)
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:

            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.update_devices(ip, hardware_id, description)
            else:
                exit(-1)


    def get_host_data(self, ip = '127.0.0.1', host_hardware_id = None, host_text_id = None):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            raw_data = requests.post(self.http_scheme + "://" + ip + self.host_api_url + "get_host_data.php", timeout = 5, data = {"host_hardware_id": host_hardware_id, "host_text_id": host_text_id})
            rt.logging.debug("raw_data", raw_data)
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:

            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.get_host_data(ip, host_hardware_id, host_text_id)
            else:
                exit(-1)


    def get_device_modules(self, ip = '127.0.0.1', host_hardware_id = None, host_text_id = None):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            raw_data = requests.post(self.http_scheme + "://" + ip + self.host_api_url + "get_host_data.php", timeout = 5, data = {"host_hardware_id": host_hardware_id, "host_text_id": host_text_id})
            rt.logging.debug("raw_data", raw_data)
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:

            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.get_device_modules(ip, host_hardware_id, host_text_id)
            else:
                exit(-1)


    def update_static_data(self, ip = '127.0.0.1', host_hardware_id = None, host_text_id = None, device_hardware_id = None, device_text_id = None, device_address = None, module_hardware_id = None, module_text_id = None, module_address = None, channel_text_id = None, host_address = None, common_description = None):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            raw_data = requests.post(self.http_scheme + "://" + ip + self.host_api_url + "update_static_data.php", timeout = 5, data = {"host_hardware_id": host_hardware_id, "host_text_id": host_text_id, "host_address": host_address, "common_description": common_description, "device_hardware_id": device_hardware_id, "device_text_id": device_text_id, "device_address": device_address, "module_hardware_id": module_hardware_id, "module_text_id": module_text_id, "module_address": module_address, "channel_text_id": channel_text_id})
            rt.logging.debug("raw_data", raw_data)
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:

            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.update_static_data(ip, host_hardware_id, host_text_id, device_hardware_id, device_text_id, device_address, module_hardware_id, module_text_id, module_address, channel_text_id, host_address, common_description)
            else:
                exit(-1)


    def get_device_data(self, ip = '127.0.0.1', device_hardware_id = None, device_text_id = None):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            raw_data = requests.post(self.http_scheme + "://" + ip + self.host_api_url + "get_device_data.php", timeout = 5, data = {"device_hardware_id": device_hardware_id, "device_text_id": device_text_id})
            rt.logging.debug("raw_data", raw_data)
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:

            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.get_device_data(ip, device_hardware_id, device_text_id)
            else:
                exit(-1)


    def update_device_data(self, ip = '127.0.0.1', host_hardware_id = None, host_text_id = None, host_address = None, host_description = None, device_hardware_id = None, device_text_id = None, module_text_id = None, channel_text_id = None):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            raw_data = requests.post(self.http_scheme + "://" + ip + self.host_api_url + "update_device_data.php", timeout = 5, data = {"host_hardware_id": host_hardware_id, "host_text_id": host_text_id, "host_address": host_address, "host_description": host_description, "device_hardware_id": device_hardware_id, "device_text_id": device_text_id, "module_text_id": module_text_id, "channel_text_id": channel_text_id})
            rt.logging.debug("raw_data", raw_data)
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:

            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.update_device_data(ip, host_hardware_id, host_text_id, host_description, device_hardware_id, device_text_id, module_text_id, channel_text_id)
            else:
                exit(-1)



class HttpClient(Http):


    def __init__(self) :

        self.env = self.get_env()
        if self.client_api_url is None: self.client_api_url = self.env['CLIENT_API_URL']

        Http.__init__(self)


    def send_request(self, ip = '127.0.0.1', start_time = -9999, end_time = -9999, duration = 10, unit = 1, delete_horizon = 3600):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            d = {"channels": tr.get_channel_range_string(self.channels), "start_time": start_time, "end_time": end_time, "duration": duration, "unit": unit, "delete_horizon": delete_horizon}
            complete_url = self.http_scheme + "://" + ip + self.client_api_url + "send_request.php"
            rt.logging.debug("complete_url", complete_url)
            raw_data = requests.post(complete_url, timeout = 5, data = d)
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.send_request(ip, start_time, end_time, duration, unit, delete_horizon)
            else:
                exit(-1)


    def get_uploaded(self, ip = '127.0.0.1', start_time = -9999, end_time = -9999, duration = 10, unit = 1, lowest_status = 0):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            rt.logging.debug("self.channels", self.channels)
            d = {"channels": tr.get_channel_range_string(self.channels), "start_time": start_time, "end_time": end_time, "duration": duration, "unit": unit, "lowest_status": lowest_status}
            rt.logging.debug("d", d)
            complete_url = self.http_scheme + "://" + ip + self.client_api_url + "get_uploaded.php"
            rt.logging.debug("complete_url", complete_url)
            raw_data = requests.post(complete_url, timeout = 5, data = d)
            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.get_uploaded(ip, start_time, end_time, duration, unit, lowest_status)
            else:
                exit(-1)


    def get_external(self, ip = ''):

        self.connect_attempts += 1
        if self.connect_attempts > 1:
            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
        try:
            raw_data = requests.get(self.http_scheme + "://" + ip + self.client_api_url, timeout = 5)

            self.connect_attempts = 0
            return raw_data
        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
            rt.logging.exception(e)
            time.sleep(10)
            if self.connect_attempts < self.max_connect_attempts:
                self.get_external(ip)
            else:
                exit(-1)
        # https://data.aishub.net/ws.php?username=AH_3265_FB8EB51C&format=0&output=csv&compress=0&latmin=59.717918&latmax=66.396283&lonmin=16.595076&lonmax=27.064446

#    def get_static_records(self, ip = '127.0.0.1', start_time = -9999, end_time = -9999, duration = 600, unit = 1, lowest_status = 0):
#
#        self.connect_attempts += 1
#        if self.connect_attempts > 1:
#            rt.logging.debug("Retrying connection, attempt " + str(self.connect_attempts))
#        try:
#            d = {"start_time": start_time, "end_time": end_time, "duration": duration, "unit": unit, "lowest_status": lowest_status}
#            raw_data = requests.post(self.http_scheme + "://" + ip + self.client_api_url + "get_static_records.php", timeout = 5, data = d)
#            self.connect_attempts = 0
#            return raw_data
#
#        except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
#            rt.logging.exception(e)
#            time.sleep(10)
#            if self.connect_attempts < self.max_connect_attempts:
#                self.get_static_records(ip, start_time, end_time, duration, unit, lowest_status)
#            else:
#                exit(-1)
#

class NativeCmems(Native):


    def __init__(self) : #, out_dir = None, out_name = None) :

        #self.out_dir = out_dir
        #self.out_name = out_name

        #self.config_filepath = config_filepath
        #self.config_filename = config_filename

        self.env = self.get_env()
        if self.service_user is None: self.service_user = self.env['SERVICE_USER']
        if self.service_pwd is None: self.service_pwd = self.env['SERVICE_PWD']

        Native.__init__(self)

        date_min = ( datetime.date.today() - datetime.timedelta( days = 0 ) ).isoformat()
        date_max = ( datetime.date.today() + datetime.timedelta( days = 0 ) ).isoformat()

        #service_id = 'BALTICSEA_ANALYSISFORECAST_WAV_003_010-TDS'
        #product_id = 'dataset-bal-analysis-forecast-wav-hourly'
        #out_dir = '/srv/dogger/sandbox/'
        #out_name = f'{service_id}.nc' #_{date_min}_{date_max}

        rt.logging.debug("getting update from %s to %s" % (date_min, date_max))
        motu_opts = {'log_level': None, 'user': self.service_user, 'pwd': self.service_pwd,
                     'auth_mode': 'cas', 'proxy':False, 'proxy_server': None,
                     'proxy_user': None, 'proxy_pwd': None,
                     'motu': 'https://nrt.cmems-du.eu/motu-web/Motu',
                     'service_id': self.service_id,
                     'product_id': self.product_id,
                     'date_min': date_min, 'date_max': date_max,
                     'latitude_min': 62.6, 'latitude_max': 62.9,
                     'longitude_min': 17.7, 'longitude_max': 18.4,
                     'depth_min': None, 'depth_max': None,
                     'variable': ['VHM0'],
                     'sync': None, 'describe': None, 'size': None,
                     'out_dir': self.file_path, 'out_name': self.out_file_name,
                     'block_size': 65536, 'socket_timeout': None, 'user_agent': None, 'outputWritten': None,
                     'console_mode': None, 'config_file': None}

        # we create a fake option parser because this is what the motu api expects:
        # a parsed result from optionparser rather than a normal dict
        self.options = None
        op = OptionParser()
        for o in motu_opts :
            op.add_option( '--' + o, dest = o, default = motu_opts[o] )
        (options, args) = op.parse_args()
        self.options = options


    def send_request(self) :

        motu_api.execute_request(self.options)
