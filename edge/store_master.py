import gateway.queue


numpy_sql = gateway.queue.NumpyFile(
    channels = {143,146,147,168,169},
    start_delay = 0,
    files_to_keep = 0)

numpy_sql.run()
