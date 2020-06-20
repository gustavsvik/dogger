import gateway.store


numpy_sql = gateway.store.NumpyFile(
    channels = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32}, 
    start_delay = 0, 
    gateway_database_connection = {"host": "host", "user": "user", "passwd": "passwd", "db": "db"}, 
    file_path = '/home/heta/Z/data/files/voltage/',
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_voltage_2.ini')

numpy_sql.run()
