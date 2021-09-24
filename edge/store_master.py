import gateway.queue


numpy_sql = gateway.queue.NumpyFile(
    channels = {143,146,147,168,169},
    start_delay = 0,
    file_path = '/home/heta/Z/data/files/',
    files_to_keep = 0,
    config_filepath = '/home/heta/Z/app/python/dogger/',
    config_filename = 'conf.ini')

numpy_sql.run()
