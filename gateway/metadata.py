#


def strip_string(val_array, label, key, default_val = ''):
    string_val = default_val
    try:
        string_val = val_array[label][key].strip('\'')
    except (ValueError, TypeError, KeyError):
        string_val = default_val
    return string_val


def to_float(val, default_val = 0.0):
    float_val = default_val
    try:
        float_val = float(val)
    except (ValueError, TypeError):
        float_val = default_val
    return float_val


def to_int(val, default_val = 0):
    int_val = default_val
    try:
        int_val = int(float(val))
    except (ValueError, TypeError):
        int_val = default_val
    return int_val


    
class Local:
    pass

    

class Configure(Local):


    def __init__(self, filepath = None, filename = None):
    
        self.config_filepath = filepath
        self.config_filename = filename


    def get(self) :

        import os
        import sys
        import json
        import configparser
        
        conf = configparser.ConfigParser()
        filepath = os.path.dirname(__file__) 
        filename = 'conf.ini' 
        if self.config_filepath is not None :
            filepath = self.config_filepath
        if self.config_filename is not None :
            filename = self.config_filename


        conf.read(os.path.join(filepath, filename))
        top_label = 'dogger'

        env = dict()

        channels = strip_string(conf, top_label, 'channels', '')
        net_file_path = strip_string(conf, top_label, 'net_file_path', '')
        local_file_path = strip_string(conf, top_label, 'local_file_path', '')
        windows_file_path = strip_string(conf, top_label, 'windows_file_path', '')
        net_archive_file_path = strip_string(conf, top_label, 'net_archive_file_path', '')
        local_archive_file_path = strip_string(conf, top_label, 'local_archive_file_path', '')
        windows_archive_file_path = strip_string(conf, top_label, 'windows_archive_file_path', '')
        #gateway_database_host = strip_string(conf, top_label, 'gateway_database_host', '')
        #gateway_database_user = strip_string(conf, top_label, 'gateway_database_user', '')
        #gateway_database_passwd = strip_string(conf, top_label, 'gateway_database_passwd', '')
        #gateway_database_name = strip_string(conf, top_label, 'gateway_database_name', '')
        gateway_database_connection = strip_string(conf, top_label, 'gateway_database_connection', '')
        ip_list = strip_string(conf, top_label, 'ip_list', '')
        cloud_api_url = strip_string(conf, top_label, 'cloud_api_url', '')
        max_connect_attempts = strip_string(conf, top_label, 'max_connect_attempts', '')
        video_unit = strip_string(conf, top_label, 'video_unit', '')
        video_res = strip_string(conf, top_label, 'video_res', '')

        sample_rate = to_float(strip_string(conf, top_label, 'sample_rate'), 0.0)
        samples_per_chan = to_float(strip_string(conf, top_label, 'samples_per_chan'), 0.0)
        video_rate = to_float(strip_string(conf, top_label, 'video_rate'), 0.0)
        #image_channel_1 = to_int(strip_string(conf, top_label, 'image_channel_1'), 0)
        #data_channel_1 = to_int(strip_string(conf, top_label, 'data_channel_1'), 0)
        #no_of_data_channels = to_int(strip_string(conf, top_label, 'no_of_data_channels'), 0)
        archive_interval = to_int(strip_string(conf, top_label, 'archive_interval'), 0)
        image_archive_interval = to_int(strip_string(conf, top_label, 'image_archive_interval'), 0)
        data_archive_interval = to_int(strip_string(conf, top_label, 'data_archive_interval'), 0)
        truncate_interval = to_int(strip_string(conf, top_label, 'truncate_interval'), 0)
        acquired_truncate_interval = to_int(strip_string(conf, top_label, 'acquired_truncate_interval'), 0)
        accumulated_delete_interval = to_int(strip_string(conf, top_label, 'accumulated_delete_interval'), 0)

        if sample_rate < 1.0 : sample_rate = 1.0
        if samples_per_chan < 1.0 : samples_per_chan = 1.0
        if video_rate < 0.0 : video_rate = 0.0
        if archive_interval < 1 : archive_interval = 1
        if image_archive_interval < 1 : image_archive_interval = 1
        if data_archive_interval < 1 : data_archive_interval = 1
        if truncate_interval < 1 : truncate_interval = 1
        if acquired_truncate_interval < 1 : acquired_truncate_interval = 1
        if accumulated_delete_interval < 1 : accumulated_delete_interval = 1

        env['CHANNELS'] = channels
        file_path = local_file_path
        if os.path.isdir(net_file_path) : file_path = net_file_path
        if sys.platform.startswith('win32') : file_path = windows_file_path
        archive_file_path = local_archive_file_path
        if os.path.isdir(net_archive_file_path) : archive_file_path = net_archive_file_path
        if sys.platform.startswith('win32') : archive_file_path = windows_archive_file_path
        env['SAMPLE_RATE'] = sample_rate
        env['SAMPLES_PER_CHAN'] = samples_per_chan
        #env['IMAGE_CHANNEL_1'] = image_channel_1
        #env['DATA_CHANNEL_1'] = data_channel_1
        #env['NO_OF_DATA_CHANNELS'] = no_of_data_channels
        env['FILE_PATH'] = file_path
        env['WINDOWS_FILE_PATH'] = windows_file_path
        env['ARCHIVE_FILE_PATH'] = archive_file_path
        env['WINDOWS_ARCHIVE_FILE_PATH'] = windows_archive_file_path
        env['TRUNCATE_INTERVAL'] = truncate_interval
        env['ACQUIRED_TRUNCATE_INTERVAL'] = acquired_truncate_interval
        env['ACCUMULATED_DELETE_INTERVAL'] = accumulated_delete_interval
        env['ARCHIVE_INTERVAL'] = archive_interval
        env['IMAGE_ARCHIVE_INTERVAL'] = image_archive_interval
        env['DATA_ARCHIVE_INTERVAL'] = data_archive_interval
        #env['GATEWAY_DATABASE_HOST'] = gateway_database_host
        #env['GATEWAY_DATABASE_USER'] = gateway_database_user
        #env['GATEWAY_DATABASE_PASSWD'] = gateway_database_passwd
        #env['GATEWAY_DATABASE_NAME'] = gateway_database_name
        try:
            env['GATEWAY_DATABASE_CONNECTION'] = json.loads(gateway_database_connection)
        except ValueError:
            env['GATEWAY_DATABASE_CONNECTION'] = gateway_database_connection
        env['IP_LIST'] = ip_list
        env['CLOUD_API_URL'] = cloud_api_url
        env['MAX_CONNECT_ATTEMPTS'] = max_connect_attempts
        env['VIDEO_UNIT'] = video_unit
        env['VIDEO_RES'] = video_res
        env['VIDEO_RATE'] = video_rate


        return env
