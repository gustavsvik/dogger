import gateway.store


numpy_sql = gateway.store.NumpyFile(
    channels = {97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112}, 
    start_delay = 0, 
    gateway_database_connection = {"host": "localhost", "user": "root", "passwd": "admin", "db": "test"}, 
    file_path = '/home/heta/Z/data/files/current/',
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_current.ini')

numpy_sql.run()
