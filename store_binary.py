import gateway.store


image_sql = gateway.store.ImageFile(
    channels = {140,141,160,161,180,181}, 
    start_delay = 0, 
    file_path = '/home/heta/Z/data/files/images/',
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf.ini')

image_sql.run()
