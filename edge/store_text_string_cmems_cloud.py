import gateway.queue


mob_store_text_sql = gateway.queue.TextJsonFile(
    channels = {154},
    start_delay = 0,
    file_path = '/srv/dogger/files/',
    files_to_keep = 0,
    config_filepath = '/srv/dogger/',
    config_filename = 'conf_cloud_db.ini')

mob_store_text_sql.run()
