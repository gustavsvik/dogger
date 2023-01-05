import gateway.link


mob_upload_sql_udp = gateway.link.SqlUdpRawValue(
    channels = {97,98,21,23,143},
    start_delay = 0,
    transmit_rate = 0.5,
    port = 61010,
    max_number = 1)

mob_upload_sql_udp.run()
