import gateway.queue


mob_store_text_sql = gateway.queue.TextStringFile(
    channels = {144,145,158},
    start_delay = 0,
    file_path = '/home/scc01/Z/data/files/',
    files_to_keep = 0)

mob_store_text_sql.run()
