import gateway.store


numpy_sql = gateway.store.NumpyFile(
    channels = {21,23,20,24,22,161,162,163,164}, 
    start_delay = 0, 
    gateway_database_connection = {"host": "host", "user": "user", "passwd": "passwd", "db": "db"}, 
    file_path = '/home/heta/Z/data/files/',
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf.ini')

numpy_sql.run()
