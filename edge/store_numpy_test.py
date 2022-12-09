import gateway.queue


numpy_sql = gateway.queue.NumpyFile(
    channels = {65534},
    start_delay = 0)

numpy_sql.run()
