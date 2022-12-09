import gateway.queue


text_sql = gateway.queue.TextFile(
    channels = {65533},
    start_delay = 0)

text_sql.run()
