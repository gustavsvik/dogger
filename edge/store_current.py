import gateway.queue


numpy_sql = gateway.queue.NumpyFile(
    channels = {97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112},
    start_delay = 0,
    files_to_keep = 0)

numpy_sql.run()
