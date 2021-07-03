import gateway.link


udp_upload_ais = gateway.link.SqlFilePosAisData(
    channels = {168,169},
    start_delay = 0,
    transmit_rate = 0.5, 
    max_age = 10,
    target_channels = {'VDM':{172:'txt'}},
    length_offset = [440, 760, 100],
    width_offset = [-750, 650, 100],
    mmsi = [999999999, 999999998, 999999997],
    vessel_name = ["SITE SERVICE FYRIS", "TEST SHIP LIZA-MARIE", "TSB PATROL SKAGERRAK"],
    call_sign = ["7SA9999", "7SA9998", "7SA9997"],
    ship_type = [55, 55, 55],
    speed = [0.1, 4.7, 17.2],
    course = [123, 58, 304],
    nav_status = [0, 0, 0],
    destination = ["TSB SITE GUSTAVSVIK", "TSB SITE GUSTAVSVIK", "TSB SITE GUSTAVSVIK"],
    file_path = '/srv/dogger/files/',
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

udp_upload_ais.run()