import gateway.queue


image_sql = gateway.queue.ScreenshotFile(
    channels = {600},
    start_delay = 0,
    file_path = '/home/heta/Z/data/files/screenshots/',
    files_to_keep = 0,
    config_filepath = '/home/heta/Z/app/python/dogger/',
    config_filename = 'conf.ini')

image_sql.run()
