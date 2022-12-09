import gateway.daqc


sie_acquire_udp_http = gateway.daqc.UdpValueHttp(
    channels = set(),
    start_delay = 0,
    transmit_rate = 10,
    port = 61010,
    max_connect_attempts = 50)

sie_acquire_udp_http.run()
