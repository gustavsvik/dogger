import gateway.link


udp_upload_ais = gateway.link.SqlFileAtonReport(
    channels = {168,169}, 
    start_delay = 0,
    transmit_rate = 0.5, 
    max_age = 10,
    target_channels = {'VDM':{173:'txt'}},
    length_offset = [585, -30],
    width_offset = [-735, -380],
    mmsi = [992659995, 992659994],
    aid_type = [19, 30],
    name = ["TSB TEST REMOTE CONTROL CENTER    ", "TSB SHORE MOBILE TEST CONTROL UNIT"],
    virtual_aid = [1, 1],
    file_path = '/srv/dogger/files/',
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

udp_upload_ais.run()
