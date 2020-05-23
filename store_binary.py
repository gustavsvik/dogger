import gateway.store

image_sql = gateway.store.ImageFile(channels = {140,141,160,161,180,181}, config_filepath = '/home/heta/Z/app/python/dogger/', config_filename = 'conf_binary.ini')
image_sql.run()
