import gateway.daqc


sie_acquire_udp_http = gateway.daqc.RawUdpFile(
    channels = {168:'npy',169:'npy'},
    start_delay = 0,
    port = 61012,
    sample_rate = 1.0)

sie_acquire_udp_http.run()
