import gateway.link


udp_upload_ais = gateway.link.SqlFileAtonReport(
    channels = {168,169}, 
    start_delay = 0,
    transmit_rate = 0.5, 
    max_age = 10,
    target_channels = {'VDM':{170:'txt'}},
    length_offset = [-400, -160, 180, 500],
    width_offset = [-300, -50, 170, 270],
    mmsi = [992659999, 992659998, 992659997, 992659996],
    aid_type = [19, 30, 30, 19],
    # TODO: Investigate why whitespace character in first position of name extension field (bits 272-360) throws exception (bitstring.CreationError "uint cannot be initialsed by a negative number." raised in aislib.py function __setattr__)
    name = ["MEASUREMENT BEACON 1 KEEP DISTANCE", "TEST AREA NO PASSAGE CH 16 7SA2026", "TEST AREA NO PASSAGE CH 16 7SA2026", "MEASUREMENT BEACON 2 KEEP DISTANCE"],
    virtual_aid = [0, 1, 1, 0],
    file_path = '/srv/dogger/files/',
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf.ini')

udp_upload_ais.run()
