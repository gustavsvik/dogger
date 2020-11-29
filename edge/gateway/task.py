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



class MaintenanceTask(Task):


    def __init__(self):

        self.env = self.get_env()
        if self.ip_list is None: self.ip_list = self.env['IP_LIST']

        Task.__init__(self)
