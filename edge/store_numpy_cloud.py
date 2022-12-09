import gateway.queue


mob_store_numpy_sql = gateway.queue.NumpyFile(
    channels = {61011,61012,168,169,170},
    start_delay = 0,
    files_to_keep = 0)

mob_store_numpy_sql.run()
