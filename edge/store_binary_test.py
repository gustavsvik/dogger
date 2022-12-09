import gateway.store


image_sql = gateway.store.ImageFile(
    channels = {65535},
    start_delay = 0)

image_sql.run()
