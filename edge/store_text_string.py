import gateway.store


text_string_sql = gateway.store.TextStringFile(
    channels = {61010}, 
    start_delay = 0, 
    file_path = '/srv/dogger/files/',
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

text_string_sql.run()
