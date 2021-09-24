import gateway.link


udp_upload_ais = gateway.link.SqlUdpAtonReport(
    channels = {168,169},
    start_delay = 0,
    transmit_rate = 0.5,
    port = 4444,
    max_age = 10,
    length_offset = [-400, -160, 180, 500],
    width_offset = [-300, -50, 170, 270],
    mmsi = [992659999, 992659998, 992659997, 992659996],
    aid_type = [19, 30, 30, 19],
    name = ["MEASUREMENT BEACON", "TEST AREA NO PASSAGE", "TEST AREA NO PASSAGE", "MEASUREMENT BEACON"],
    virtual_aid = [0, 1, 1, 0],
    config_filepath = '/home/heta/Z/app/python/dogger/',
    config_filename = 'conf_udp_local.ini')

udp_upload_ais.run()
