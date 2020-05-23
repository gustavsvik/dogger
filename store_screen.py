import gateway.store

image_sql = gateway.store.ImageFile(config_filepath = '/home/heta/Z/app/python/dogger/', config_filename = 'conf_screen.ini', channels = {600})
image_sql.run()
