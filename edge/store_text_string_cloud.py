import gateway.queue


mob_store_text_sql = gateway.queue.TextJsonFile(
    channels = {61010,61013,170,148,172,173,149,152,156},
    start_delay = 0,
    files_to_keep = 0)

mob_store_text_sql.run()
