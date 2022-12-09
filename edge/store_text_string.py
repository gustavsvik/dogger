import gateway.queue


mob_store_text_sql = gateway.queue.TextStringFile(
    channels = {142,150,151,155,157,162,163,164,167,171},
    start_delay = 0,
    files_to_keep = 0)

mob_store_text_sql.run()
