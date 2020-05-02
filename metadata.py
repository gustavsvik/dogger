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
        import configparser
        import runtime

        conf = configparser.ConfigParser()
        filepath = os.path.dirname(__file__) #Configure.CONFIG_FILEPATH
        filename = 'conf.ini' #Configure.CONFIG_FILENAME
        if self.config_filepath is not None :
            filepath = self.config_filepath
        if self.config_filename is not None :
            filename = self.config_filename
        conf.read(os.path.join(filepath, filename))
        top_label = 'dogger'

        env = dict()

        net_store_path = strip_string(conf, top_label, 'net_store_path', '')
        local_store_path = strip_string(conf, top_label, 'local_store_path', '')
        windows_store_path = strip_string(conf, top_label, 'windows_store_path', '')
        net_archive_path = strip_string(conf, top_label, 'net_archive_path', '')
        local_archive_path = strip_string(conf, top_label, 'local_archive_path', '')
        windows_archive_path = strip_string(conf, top_label, 'windows_archive_path', '')
        store_database_host = strip_string(conf, top_label, 'store_database_host', '')
        store_database_user = strip_string(conf, top_label, 'store_database_user', '')
        store_database_passwd = strip_string(conf, top_label, 'store_database_passwd', '')
        store_database_db = strip_string(conf, top_label, 'store_database_db', '')

        sample_rate = to_float(strip_string(conf, top_label, 'sample_rate'), 0.0)
        samples_per_chan = to_float(strip_string(conf, top_label, 'samples_per_chan'), 0.0)
        image_channel_1 = to_int(strip_string(conf, top_label, 'image_channel_1'), 0)
        data_channel_1 = to_int(strip_string(conf, top_label, 'data_channel_1'), 0)
        no_of_data_channels = to_int(strip_string(conf, top_label, 'no_of_data_channels'), 0)
        archive_interval = to_int(strip_string(conf, top_label, 'archive_interval'), 0)
        image_archive_interval = to_int(strip_string(conf, top_label, 'image_archive_interval'), 0)
        data_archive_interval = to_int(strip_string(conf, top_label, 'data_archive_interval'), 0)
        truncate_interval = to_int(strip_string(conf, top_label, 'truncate_interval'), 0)
        acquired_truncate_interval = to_int(strip_string(conf, top_label, 'acquired_truncate_interval'), 0)
        accumulated_delete_interval = to_int(strip_string(conf, top_label, 'accumulated_delete_interval'), 0)

        if sample_rate < 1 : sample_rate = 1
        if samples_per_chan < 1 : samples_per_chan = 1
        if archive_interval < 1 : archive_interval = 1
        if image_archive_interval < 1 : image_archive_interval = 1
        if data_archive_interval < 1 : data_archive_interval = 1
        if truncate_interval < 1 : truncate_interval = 1
        if acquired_truncate_interval < 1 : acquired_truncate_interval = 1
        if accumulated_delete_interval < 1 : accumulated_delete_interval = 1

        store_path = local_store_path
        if os.path.isdir(net_store_path) : store_path = net_store_path
        archive_path = local_archive_path
        if os.path.isdir(net_archive_path) : archive_path = net_archive_path

        env['SAMPLE_RATE'] = sample_rate
        env['SAMPLES_PER_CHAN'] = samples_per_chan
        env['IMAGE_CHANNEL_1'] = image_channel_1
        env['DATA_CHANNEL_1'] = data_channel_1
        env['NO_OF_DATA_CHANNELS'] = no_of_data_channels
        env['STORE_PATH'] = store_path
        env['WINDOWS_STORE_PATH'] = windows_store_path
        env['ARCHIVE_PATH'] = archive_path
        env['WINDOWS_ARCHIVE_PATH'] = windows_archive_path
        env['TRUNCATE_INTERVAL'] = truncate_interval
        env['ACQUIRED_TRUNCATE_INTERVAL'] = acquired_truncate_interval
        env['ACCUMULATED_DELETE_INTERVAL'] = accumulated_delete_interval
        env['ARCHIVE_INTERVAL'] = archive_interval
        env['IMAGE_ARCHIVE_INTERVAL'] = image_archive_interval
        env['DATA_ARCHIVE_INTERVAL'] = data_archive_interval
        env['STORE_DATABASE_HOST'] = store_database_host
        env['STORE_DATABASE_USER'] = store_database_user
        env['STORE_DATABASE_PASSWD'] = store_database_passwd
        env['STORE_DATABASE_DB'] = store_database_db

        return env
