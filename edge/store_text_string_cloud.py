import gateway.store


mob_store_text_sql = gateway.store.TextStringFile(
    channels = {61010,61013,170,148}, 
    start_delay = 0, 
    file_path = '/srv/dogger/files/',
    files_to_keep = 0,
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

mob_store_text_sql.run()
