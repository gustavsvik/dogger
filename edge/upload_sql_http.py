import gateway.link


sie_upload_sql_http = gateway.link.SqlHttp(
    channels = {168,169,170,148,149,158,152,154,156,172,173,61011,61012},
    start_delay = 0,
    transmit_rate = 0.5,
    max_age = 10,
    max_connect_attempts = 50)

sie_upload_sql_http.run()
