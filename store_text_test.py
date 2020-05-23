import gateway.store

text_sql = gateway.store.TextFile(channels = {65533}, config_filepath = '/home/heta/Z/app/python/dogger/', config_filename = 'conf.ini')
text_sql.run()
