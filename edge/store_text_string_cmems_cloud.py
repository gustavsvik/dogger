import gateway.queue


mob_store_text_sql = gateway.queue.TextJsonFile(
    channels = {154},
    start_delay = 0,
    files_to_keep = 0,
    config_filename = 'conf_cloud_db.ini')

mob_store_text_sql.run()
