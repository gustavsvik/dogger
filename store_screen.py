import gateway.store


image_sql = gateway.store.ScreenshotFile(
    channels = {600}, 
    start_delay = 0, 
    gateway_database_connection = {"host": "host", "user": "user", "passwd": "passwd", "db": "db"}, 
    file_path = '/home/heta/Z/data/files/screenshots/',
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_screen.ini')

image_sql.run()
