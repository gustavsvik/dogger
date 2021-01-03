import gateway.store


numpy_sql = gateway.store.NumpyFile(
    channels = {143}, 
    start_delay = 0, 
    file_path = '/home/heta/Z/data/files/',
    files_to_keep = 0,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf.ini')

numpy_sql.run()
