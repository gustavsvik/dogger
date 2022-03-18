import gateway.queue


mob_store_numpy_sql = gateway.queue.NumpyFileMsgpack(
    channels = {153},
    start_delay = 0,
    file_path = '/srv/dogger/files/',
    files_to_keep = 0,
    config_filepath = '/srv/dogger/',
    config_filename = 'conf.ini')

mob_store_numpy_sql.run()
