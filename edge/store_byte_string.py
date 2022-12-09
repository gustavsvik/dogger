import gateway.queue


mob_store_byte_sql = gateway.queue.ByteStringFile(
    channels = {155},
    start_delay = 0,
    files_to_keep = 0)

mob_store_byte_sql.run()
