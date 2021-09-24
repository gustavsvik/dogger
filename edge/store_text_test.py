import gateway.queue


text_sql = gateway.queue.TextFile(
    channels = {65533},
    start_delay = 0,
    file_path = '/home/heta/Z/data/files/',
    config_filepath = '/home/heta/Z/app/python/dogger/',
    config_filename = 'conf.ini')

text_sql.run()
