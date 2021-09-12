#

import multiprocessing
import threading
import importlib

import gateway.metadata as md
import gateway.utils as ut



def run_task(module_name, class_name, arg) :

    the_class = getattr(importlib.import_module(module_name), class_name)
    instance = ut.instance_from_yaml_string( the_class, arg )
    instance.run()


def run_process_array(module_name, class_name, arg_strings) :

    processes = [ multiprocessing.Process(target = run_task, args = (module_name, class_name, arg_string)) for arg_string in arg_strings ]
    for process in processes :
        process.start()


def run_thread_array(module_name, class_name, arg_strings) :

    threads = [ threading.Thread(target = run_task, args = (module_name, class_name, arg_string)) for arg_string in arg_strings ]
    for thread in threads :
        thread.start()



class Task:


    def __init__(self):

        self.env = self.get_env()

        if self.start_delay is None: self.start_delay = self.env['START_DELAY']


    def get_env(self):

        config = md.Configure(filepath = self.config_filepath, filename = self.config_filename)
        env = config.get()

        return env



class ProcessDataTask(Task):


    def __init__(self):

        self.env = self.get_env()
        if self.channels is None: self.channels = self.env['CHANNELS']

        Task.__init__(self)



class AcquireControlTask(ProcessDataTask):


    def __init__(self):

        self.env = self.get_env()
        if self.sample_rate is None: self.sample_rate = self.env['SAMPLE_RATE']

        ProcessDataTask.__init__(self)



class LinkTask(ProcessDataTask):


    def __init__(self):

        self.env = self.get_env()
        if self.transmit_rate is None: self.transmit_rate = self.env['TRANSMIT_RATE']

        ProcessDataTask.__init__(self)



class IpTask(LinkTask):


    def __init__(self):

        self.env = self.get_env()
        if self.ip_list is None: self.ip_list = self.env['IP_LIST']

        #self.url_scheme = None
        #url_prefix = self.ip_list.split(':')[0]
        #if url_prefix in ["http", "https"] :
        #    self.url_scheme = url_prefix

        LinkTask.__init__(self)



class HttpTask(IpTask):


    def __init__(self):

        self.env = self.get_env()
        if self.http_scheme is None: self.http_scheme = self.env['HTTP_SCHEME']

        IpTask.__init__(self)



class MaintenanceTask(Task):


    def __init__(self):

        self.env = self.get_env()
        if self.ip_list is None: self.ip_list = self.env['IP_LIST']
        if self.http_scheme is None: self.http_scheme = self.env['HTTP_SCHEME']
        if self.max_connect_attempts is None: self.max_connect_attempts = self.env['MAX_CONNECT_ATTEMPTS']

        #self.url_scheme = None
        #url_prefix = self.ip_list.split(':')[0]
        #if url_prefix in ["http", "https"] :
        #    self.url_scheme = url_prefix

        Task.__init__(self)

        self.connect_attempts = 0
