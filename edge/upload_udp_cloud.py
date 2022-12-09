import gateway.link


mob_upload_sql_udp = gateway.link.SqlUdpRawValue(
    channels = {61011,61012},
    start_delay = 0,
    transmit_rate = 1,
    port = 61010,
    max_age = 10)

mob_upload_sql_udp.run()
