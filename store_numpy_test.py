import gateway.store

numpy_sql = gateway.store.NumpyFile(channels = {65534}, config_filepath = '/home/heta/Z/app/python/dogger/', config_filename = 'conf.ini')
numpy_sql.run()
