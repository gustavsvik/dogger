import store.host


image_sql = store.host.ImageFile(channels = {65535}, config_filename = 'conf.ini')
image_sql.run()
