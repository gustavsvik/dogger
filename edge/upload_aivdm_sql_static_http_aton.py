import gateway.link


udp_upload_ais = gateway.link.SqlHttpUpdateStatic(
    channels = {170}, 
    start_delay = 0,
    transmit_rate = 0.2, 
    max_age = 10,
    max_connect_attempts = 50, 
    message_formats = [ {"message": {"type":21}, 
                     "aid_type":     { "type":"int", "novalue":0 }, 
                     "name":         { "type":"str" }, 
                     "accuracy":     { "type":"bool" },
                     "lon":          { "type":"float", "round":5, "novalue":181 }, 
                     "lat":          { "type":"float", "round":5, "novalue":91 }, 
                     "to_bow":       { "type":"int", "novalue":0, "overflow":511 },
                     "to_stern":     { "type":"int", "novalue":0, "overflow":511 },
                     "to_port":      { "type":"int", "novalue":0, "overflow":63 },
                     "to_starboard": { "type":"int", "novalue":0, "overflow":63 },
                     "second":       { "type":"int", "novalue":60 },
                     "virtual_aid":  { "type":"bool" },
                     "name_extension":{ "type":"str" }, 
                     "complete_name":{ "function":{"name":"get_name_plus_extension", "args":{"name", "name_extension"}} },
                     "device_hardware_id": { "function":{"name":"create_key", "args":{"mmsi"}} },
                     "host_hardware_id":{ "function":{"name":"create_key", "args":{"mmsi"}} },
                     "icon_filename":{ "function":{"name":"get_png_name_by_aton_type", "args":{"aid_type", "virtual_aid"}} },
                     "image_filename":{ "function":{"name":"get_png_name_by_key", "args":{"mmsi"}} } } ],
    config_filepath = '/srv/dogger/', 
    config_filename = 'conf_cloud_db.ini')

udp_upload_ais.run()
