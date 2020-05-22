import store.host


text_sql = store.host.TextFile(channels = {65533}, config_filename = 'conf.ini')
text_sql.run()
