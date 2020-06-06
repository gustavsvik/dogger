#

import gateway.metadata as md



class Task:


    def __init__(self):

        self.env = self.get_env()
        if self.channels is None: self.channels = self.env['CHANNELS']
        if self.start_delay is None: self.start_delay = self.env['START_DELAY']


    def get_env(self):

        config = md.Configure(filepath = self.config_filepath, filename = self.config_filename)
        env = config.get()

        return env
        
        

class StoreUplink(Task):


    def __init__(self):

        self.env = self.get_env()
        if self.gateway_database_connection is None: self.gateway_database_connection = self.env['GATEWAY_DATABASE_CONNECTION']

        Task.__init__(self)

        

class AcquireControl(Task):


    def __init__(self):

        self.env = self.get_env()
        
        if self.sample_rate is None: self.sample_rate = self.env['SAMPLE_RATE']

        Task.__init__(self)
