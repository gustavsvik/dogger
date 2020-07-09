import gateway.store


image_sql = gateway.store.ScreenshotFile(
    channels = {600}, 
    start_delay = 0, 
    file_path = '/home/heta/Z/data/files/screenshots/',
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf.ini')

image_sql.run()
