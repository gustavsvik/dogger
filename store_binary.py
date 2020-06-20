import gateway.store


image_sql = gateway.store.ImageFile(
    channels = {140,141,160,161,180,181}, 
    start_delay = 0, 
    gateway_database_connection = {"host": "host", "user": "user", "passwd": "passwd", "db": "db"}, 
    file_path = '/home/heta/Z/data/files/images/',
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_binary.ini')

image_sql.run()
