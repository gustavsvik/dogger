import store.host


image_sql = store.host.ImageFile(channels = {140,141,160,161,180,181}, config_filename = 'conf.ini')
image_sql.run()
