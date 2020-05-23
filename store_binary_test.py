import gateway.store

image_sql = gateway.store.ImageFile(channels = {65535}, config_filepath = '/home/heta/Z/app/python/dogger/', config_filename = 'conf.ini')
image_sql.run()
