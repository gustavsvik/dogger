import gateway.store


numpy_sql = gateway.store.NumpyFile(
    channels = {61011,61012}, 
    start_delay = 0, 
    file_path = '/srv/dogger/files/',
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

numpy_sql.run()
