import gateway.store


text_sql = gateway.store.TextFile(
    channels = {65533}, 
    start_delay = 0, 
    gateway_database_connection = {"host": "localhost", "user": "root", "passwd": "admin", "db": "test"}, 
    file_path = '/home/heta/Z/data/files/',
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf.ini')

text_sql.run()
