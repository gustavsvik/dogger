#

class Local:
    pass
 
class Configure(Local):

    import os
    
    #DATA_FILEPATH = '/home/heta/Z/data/files/'
    #WINDOWS_FILEPATH = 'Z:/data/files/'
    CONFIG_FILEPATH = os.path.dirname(__file__)
    CONFIG_FILENAME = 'conf.ini'

    def __init__(self, config_filename = None):
        self.config_filename = config_filename

    #def get_data_filepath(self):
    #    self.data_filepath = Configure.DATA_FILEPATH
    #    return self.data_filepath

    #def get_windows_filepath(self):
    #    self.windows_filepath = Configure.WINDOWS_FILEPATH
    #    return self.windows_filepath

    def get(self) :

        import os
        import configparser
        import runtime

        conf = configparser.ConfigParser()
        conf.read(os.path.join(Configure.CONFIG_FILEPATH, Configure.CONFIG_FILENAME))
        top_label = 'dogger'

        env = dict()

        net_store_path = ''
        local_store_path = ''
        windows_store_path = ''
        net_archive_path = ''
        local_archive_path = ''
        windows_archive_path = ''
        sample_rate = 0
        samples_per_chan = 0
        image_channel_1 = 0
        data_channel_1 = 0
        no_of_data_channels = 0
        archive_interval = 0
        image_archive_interval = 0
        data_archive_interval = 0
        truncate_interval = 0
        acquired_truncate_interval = 0
        accumulated_delete_interval = 0
        store_database_host = ''
        store_database_user = ''
        store_database_passwd = ''
        store_database_db = ''

        net_store_path = conf[top_label]['net_store_path'].strip('\'')
        local_store_path = conf[top_label]['local_store_path'].strip('\'')
        windows_store_path = conf[top_label]['windows_store_path'].strip('\'')
        net_archive_path = conf[top_label]['net_archive_path'].strip('\'')
        local_archive_path = conf[top_label]['local_archive_path'].strip('\'')
        windows_archive_path = conf[top_label]['windows_archive_path'].strip('\'')
        store_database_host = conf[top_label]['store_database_host'].strip('\'')
        store_database_user = conf[top_label]['store_database_user'].strip('\'')
        store_database_passwd = conf[top_label]['store_database_passwd'].strip('\'')
        store_database_db = conf[top_label]['store_database_db'].strip('\'')
        try :
            sample_rate = float(conf[top_label]['sample_rate'].strip('\''))
        except (TypeError, ValueError, KeyError) as e :
            runtime.logging.exception(e)
        try :
            samples_per_chan = float(conf[top_label]['samples_per_chan'].strip('\''))
        except (TypeError, ValueError, KeyError) as e :
            runtime.logging.exception(e)
        try :
            image_channel_1 = int(conf[top_label]['image_channel_1'].strip('\''))
        except (TypeError, ValueError, KeyError) as e :
            runtime.logging.exception(e)
        try :
            data_channel_1 = int(conf[top_label]['data_channel_1'].strip('\''))
        except (TypeError, ValueError, KeyError) as e :
            runtime.logging.exception(e)
        try :
            no_of_data_channels = int(conf[top_label]['no_of_data_channels'].strip('\''))
        except (TypeError, ValueError, KeyError) as e :
            runtime.logging.exception(e)
        try :
            archive_interval = int(conf[top_label]['archive_interval'].strip('\''))
        except (TypeError, ValueError, KeyError) as e :
            runtime.logging.exception(e)
        try :
            image_archive_interval = int(conf[top_label]['image_archive_interval'].strip('\''))
        except (TypeError, ValueError, KeyError) as e :
            runtime.logging.exception(e)
        try :
            data_archive_interval = int(conf[top_label]['data_archive_interval'].strip('\''))
        except (TypeError, ValueError, KeyError) as e :
            runtime.logging.exception(e)
        try :
            truncate_interval = int(conf[top_label]['truncate_interval'].strip('\''))
        except (TypeError, ValueError, KeyError) as e :
            runtime.logging.exception(e)
        try :
            acquired_truncate_interval = int(conf[top_label]['acquired_truncate_interval'].strip('\''))
        except (TypeError, ValueError, KeyError) as e :
            runtime.logging.exception(e)
        try :
            accumulated_delete_interval = int(conf[top_label]['accumulated_delete_interval'].strip('\''))
        except (TypeError, ValueError, KeyError) as e :
            runtime.logging.exception(e)

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
