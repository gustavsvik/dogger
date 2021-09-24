import gateway.queue


mob_store_numpy_sql = gateway.queue.NumpyFile(
    channels = {61011,61012,168,169,170},
    start_delay = 0,
    file_path = '/srv/dogger/files/',
    files_to_keep = 0,
    config_filepath = '/srv/dogger/',
    config_filename = 'conf.ini')

mob_store_numpy_sql.run()
