import gateway.daqc


mob_acquire_nmea_udp_file = gateway.daqc.NmeaUdpFile(
    channels = {'GGA':{61010:'txt',61011:'npy',61012:'npy'}, 'VTG':{61013:'txt'}},
    port = 50000,
    start_delay = 0,
    sample_rate = 1.0)

mob_acquire_nmea_udp_file.run()
