import gateway.link


mob_upload_sql_udp = gateway.link.SqlUdpRawValue(
    channels = {143,98,146,147},
    start_delay = 0,
    transmit_rate = 0.5,
    port = 61010,
    max_age = 10)

mob_upload_sql_udp.run()
