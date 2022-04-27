import gateway.queue


mob_store_text_sql = gateway.queue.TextStringFile(
    channels = {144,145},
    start_delay = 0,
    file_path = '/home/scc01/Z/data/files/',
    files_to_keep = 0,
    config_filepath = '/home/scc01/Z/dogger/',
    config_filename = 'conf.ini')

mob_store_text_sql.run()
