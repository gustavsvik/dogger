#

import time
import pymysql
import numpy
import sys
import os
import math
import metadata.local


class Host:

    def __init__(self):

        config = metadata.local.Configure()
        self.dataFilepath = config.getDataFilePath()


class Sql(Host):

    def __init__(self):
        pass



class Npy(Sql):

    def __init__(self):
        pass
