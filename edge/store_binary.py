import gateway.queue


image_sql = gateway.queue.ImageFile(
    channels = {140,141,160,161,180,181,200,220},
    start_delay = 0,
    file_path = '/home/heta/Z/data/files/images/',
    files_to_keep = 0,
    config_filepath = '/home/heta/Z/app/python/dogger/',
    config_filename = 'conf.ini')

image_sql.run()
