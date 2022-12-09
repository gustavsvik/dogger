import gateway.link


mob_upload_sql_udp = gateway.link.SqlUdpRawBytes(
    channels = {162,163,171},
    start_delay = 0,
    #transmit_rate = 1,
    transmit_rate = 0.5,
    port = 61011,
    max_age = 10)

mob_upload_sql_udp.run()
