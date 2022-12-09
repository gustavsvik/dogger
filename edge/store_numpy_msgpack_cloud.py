import gateway.queue


mob_store_numpy_sql = gateway.queue.NumpyFileMsgpack(
    channels = {153},
    start_delay = 0,
    files_to_keep = 0)

mob_store_numpy_sql.run()
