#

class Local:
    pass
 
class Configure(Local):

    DATA_FILEPATH = "../../../data/files/"
    CONFIG_FILENAME = "loggingconfig.ini"

    def __init__(self, config_filename = None):
        self.config_filename = config_filename

    def get_data_filepath(self):
        self.data_filepath = Configure.DATA_FILEPATH
        return self.data_filepath
