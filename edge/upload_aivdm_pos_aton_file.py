import gateway.link


udp_upload_ais = gateway.link.SqlFileAtonReport(
    channels = {168,169},
    start_delay = 0,
    transmit_rate = 0.5,
    max_age = 10,
    target_channels = {'VDM':{170:'txt'}},
    length_offset = [-400, -160, 180, 500],
    width_offset = [-285, -50, 170, 275],
    mmsi = [992659999, 992659998, 992659997, 992659996],
    aid_type = [19, 30, 30, 19],
    name = ["MEASURE BEACON 01, KEEP DISTANCE! ", "TEST AREA,  NO ENTRY! CH16 7SA9999", "TEST AREA,  NO ENTRY! CH16 7SA9999", "MEASURE BEACON 02, KEEP DISTANCE! "],
    virtual_aid = [0, 1, 1, 0] )

udp_upload_ais.run()
