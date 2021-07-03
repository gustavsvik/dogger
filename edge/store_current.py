import gateway.queue


numpy_sql = gateway.queue.NumpyFile(
    channels = {97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112}, 
    start_delay = 0, 
    file_path = '/home/heta/Z/data/files/current/',
    files_to_keep = 0,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf.ini')

numpy_sql.run()
