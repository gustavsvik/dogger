import gateway.store


mob_store_text_sql = gateway.store.TextStringFile(
    channels = {142,144,145,146,147,162}, 
    start_delay = 0, 
    file_path = '/home/heta/Z/data/files/', 
    files_to_keep = 0,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf.ini')

mob_store_text_sql.run()
