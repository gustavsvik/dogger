import gateway.queue


numpy_sql = gateway.queue.NumpyFile(
    channels = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32},
    start_delay = 0,
    file_path = '/home/heta/Z/data/files/voltage/',
    files_to_keep = 0,
    config_filepath = '/home/heta/Z/app/python/dogger/',
    config_filename = 'conf.ini')

numpy_sql.run()
