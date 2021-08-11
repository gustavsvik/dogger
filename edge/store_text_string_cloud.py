import gateway.queue


mob_store_text_sql = gateway.queue.TextJsonFile(
    channels = {61010,61013,170,148,172,173,149,152}, 
    start_delay = 0, 
    file_path = '/srv/dogger/files/',
    files_to_keep = 0,
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

mob_store_text_sql.run()
