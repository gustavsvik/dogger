import gateway.store


image_sql = gateway.store.ImageFile(
    channels = {65535},
    start_delay = 0,
    file_path = '/home/heta/Z/data/files/',
    config_filepath = '/home/heta/Z/app/python/dogger/',
    config_filename = 'conf.ini')

image_sql.run()
