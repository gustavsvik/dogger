import gateway.link


udp_upload_ais = gateway.link.SqlUdpAtonReport(
    channels = {61011,61012}, 
    start_delay = 0,
    transmit_rate = 1, 
    port = 4444,
    max_age = 10,
    length_offset = [50, -20, -20, 50, 30],
    width_offset = [20, 20, -20, -20, 0],
    mmsi = [992659999, 992659998, 992659997, 992659996, 992659995],
    aid_type = [30, 30, 30, 30, 28],
    name = ["TEST AREA DEMARC. 1", "TEST AREA DEMARC. 2", "TEST AREA DEMARC. 3", "TEST AREA DEMARC. 4", "MEASUREMENT BUOY"],
    virtual_aid = [1, 1, 1, 1, 0],
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf_udp_local.ini')

udp_upload_ais.run()
