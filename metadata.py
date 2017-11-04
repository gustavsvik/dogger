#

class Local:
    pass
 
class Configure(Local):

    DATA_FILEPATH = "../../../data/files/"
    CONFIG_FILENAME = "loggingconfig.ini"

    def __init__(self, configFilename = None):
        self.configFilename = configFilename

    def getDataFilePath(self):
        self.dataFilePath = Configure.DATA_FILEPATH
        return self.dataFilePath
