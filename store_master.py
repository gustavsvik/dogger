import store.host


numpy_sql = store.host.NumpyFile(channels = {21,23,20,24,22,161,162,163,164}, config_filename = 'conf.ini')
numpy_sql.run()
