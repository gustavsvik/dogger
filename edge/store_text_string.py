import gateway.store


mob_store_text_sql = gateway.store.TextStringFile(
    channels = {142,144,145,150,151,162,163,167,171},
    start_delay = 0, 
    file_path = '/home/heta/Z/data/files/', 
    files_to_keep = 0,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf.ini')

mob_store_text_sql.run()
