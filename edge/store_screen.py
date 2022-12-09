import gateway.queue


image_sql = gateway.queue.ScreenshotFile(
    channels = {600},
    start_delay = 0,
    files_to_keep = 0)

image_sql.run()
