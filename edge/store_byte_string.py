import gateway.queue


mob_store_byte_sql = gateway.queue.ByteStringFile(
    channels = {155},
    start_delay = 0,
    file_path = '/home/scc01/Z/data/files/',
    files_to_keep = 0,
    config_filepath = '/home/scc01/Z/dogger/',
    config_filename = 'conf.ini')

mob_store_byte_sql.run()
