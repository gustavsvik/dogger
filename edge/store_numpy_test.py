import gateway.queue


numpy_sql = gateway.queue.NumpyFile(
    channels = {65534},
    start_delay = 0,
    file_path = '/home/heta/Z/data/files/',
    config_filepath = '/home/heta/Z/app/python/dogger/',
    config_filename = 'conf.ini')

numpy_sql.run()
