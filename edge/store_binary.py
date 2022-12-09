import gateway.queue


image_sql = gateway.queue.ImageFile(
    channels = {140,141,160,161,180,181,200,220},
    start_delay = 0,
    files_to_keep = 0)

image_sql.run()
