import store.host


numpy_sql = store.host.NumpyFile(channels = {65534}, config_filename = 'conf.ini')
numpy_sql.run()
