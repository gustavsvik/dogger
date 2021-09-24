import gateway.link


udp_upload_ais = gateway.link.SqlHttpUpdateDevice(
    channels = {144},
    start_delay = 0,
    transmit_rate = 0.5,
    max_age = 10,
    max_connect_attempts = 50,
    message_formats = [ {"message": {"type":[1,2,3]},
                     "mmsi":         {"type":"str"},
                     "status":       { "type":"int", "novalue":15 },
                     "turn":         { "type":"float", "round":1, "novalue":-128, "underflow":-127, "overflow":127 },
                     "speed":        { "type":"float", "round":1, "novalue":1023, "overflow":1022 },
                     "accuracy":     { "type":"bool" },
                     "lon":          { "type":"float", "round":5, "novalue":181 },
                     "lat":          { "type":"float", "round":5, "novalue":91 },
                     "course":       { "type":"float", "round":1, "novalue":360 },
                     "heading":      { "type":"float", "round":1, "novalue":511 },
                     "second":       { "type":"int", "novalue":[61,62,63], "nosensor":60 },
                     "device_hardware_id": { "function":{"name":"get_device_hardware_id", "args":{"mmsi"}} } },
                    {"message": {"type":18},
                     "mmsi":         {"type":"str"},
                     "speed":        { "type":"float", "round":1, "novalue":1023, "overflow":1022 },
                     "accuracy":     { "type":"bool" },
                     "lon":          { "type":"float", "round":5, "novalue":181 },
                     "lat":          { "type":"float", "round":5, "novalue":91 },
                     "course":       { "type":"float", "round":1, "novalue":360 },
                     "heading":      { "type":"float", "round":1, "novalue":511 },
                     "second":       { "type":"int", "novalue":[61,62,63], "nosensor":60 },
                     "device_hardware_id": { "function":{"name":"get_device_hardware_id", "args":{"mmsi"}} } },
                    {"message": {"type":9},
                     "mmsi":         {"type":"str"},
                     "alt":          { "type":"int", "novalue":4095, "overflow":4094 },
                     "speed":        { "type":"int", "novalue":1023, "overflow":1022 },
                     "accuracy":     { "type":"bool" },
                     "lon":          { "type":"float", "round":5, "novalue":181 },
                     "lat":          { "type":"float", "round":5, "novalue":91 },
                     "course":       { "type":"float", "round":1, "novalue":360 },
                     "second":       { "type":"int", "novalue":[61,62,63], "nosensor":60 },
                     "device_hardware_id": { "function":{"name":"get_device_hardware_id", "args":{"mmsi"}} } },
                    {"message": {"type":4},
                     "mmsi":         {"type":"str"},
                     "year":         { "type":"int", "novalue":0 },
                     "month":        { "type":"int", "novalue":0 },
                     "day":          { "type":"int", "novalue":0 },
                     "hour":         { "type":"int", "novalue":24 },
                     "minute":       { "type":"int", "novalue":60 },
                     "second":       { "type":"int", "novalue":60 },
                     "accuracy":     { "type":"bool" },
                     "lon":          { "type":"float", "round":5, "novalue":181 },
                     "lat":          { "type":"float", "round":5, "novalue":91 },
                     "device_hardware_id": { "function":{"name":"get_device_hardware_id", "args":{"mmsi"}} } },
                    {"message": {"type":8, "fid":31, "start_pos":56},
                     "mmsi":         { "type":"str", "values":["002655619"]},
                     "lon":          { "bits":[ 56, 80], "type":"I3", "div":60000, "novalue":181 },
                     "lat":          { "bits":[ 81,104], "type":"I3", "div":60000, "novalue":91 },
                     "accuracy":     { "bits":[105,105], "type":"b" },
                     "day":          { "bits":[106,110], "type":"u", "novalue":0 },
                     "hour":         { "bits":[111,115], "type":"u", "novalue":24 },
                     "minute":       { "bits":[116,121], "type":"u", "novalue":60 },
                     "wspeed":       { "bits":[122,128], "type":"u", "novalue":127, "overflow": 126 },
                     "wgust":        { "bits":[129,135], "type":"u", "novalue":127, "overflow": 126 },
                     "wdir":         { "bits":[136,144], "type":"u", "novalue":360 },
                     "wgustdir":     { "bits":[145,153], "type":"u", "novalue":360 },
                     "airtemp":      { "bits":[154,164], "type":"I1", "round":1, "div":10, "novalue":-1024 },
                     "humidity":     { "bits":[165,171], "type":"u", "novalue":101 },
                     "dewpoint":     { "bits":[172,181], "type":"I1", "round":2, "div":10, "novalue":501 },
                     "pressure":     { "bits":[182,190], "type":"u", "novalue":511, "underflow": 0, "overflow": 402, "add": 799 },
                     "pressuretend": { "bits":[191,192], "type":"e", "novalue":3 },
                     "visibility":   { "bits":[193,200], "type":"U1", "round":1, "div":10, "novalue":127, "overflow_var_flag":[193,193] },
                     "waterlevel":   { "bits":[201,212], "type":"U2", "round":3, "div":100, "add":-10, "novalue":4001 },
                     "leveltrend":   { "bits":[213,214], "type":"e", "novalue":3 },
                     "cspeed":       { "bits":[215,222], "type":"U1", "round":1, "div":10, "novalue":255, "overflow": 251 },
                     "cdir":         { "bits":[223,231], "type":"u", "novalue":360 },
                     "cspeed2":      { "bits":[232,239], "type":"U1", "round":1, "div":10, "novalue":255, "overflow": 251 },
                     "cdir2":        { "bits":[240,248], "type":"u", "novalue":360 },
                     "cdepth2":      { "bits":[249,253], "type":"U1", "round":1, "div":10, "novalue":31 },
                     "cspeed3":      { "bits":[254,261], "type":"U1", "round":1, "div":10, "novalue":255, "overflow": 251 },
                     "cdir3":        { "bits":[262,270], "type":"u", "novalue":360 },
                     "cdepth3":      { "bits":[271,275], "type":"U1", "round":1, "div":10, "novalue":31 },
                     "waveheight":   { "bits":[276,283], "type":"U1", "round":1, "div":10, "novalue":255, "overflow": 251 },
                     "waveperiod":   { "bits":[284,289], "type":"u", "novalue":63 },
                     "wavedir":      { "bits":[290,298], "type":"u", "novalue":360 },
                     "swellheight":  { "bits":[299,306], "type":"U1", "round":1, "div":10, "novalue":255, "overflow": 251 },
                     "swellperiod":  { "bits":[307,312], "type":"u", "novalue":63 },
                     "swelldir":     { "bits":[313,321], "type":"u", "novalue":360 },
                     "seastate":     { "bits":[322,325], "type":"e", "novalue":13 },
                     "watertemp":    { "bits":[326,335], "type":"I1", "round":1, "div":10, "novalue":501 },
                     "preciptype":   { "bits":[336,338], "type":"e", "novalue":7 },
                     "salinity":     { "bits":[339,347], "type":"U1", "round":1, "div":10, "novalue":510, "nosensor":511 },
                     "ice":          { "bits":[348,349], "type":"e", "novalue":3 },
                     "olc":          { "function":{"name":"get_open_location_code", "args":{"lat", "lon"}} },
                     "device_hardware_id": { "function":{"name":"get_device_hardware_id", "args":{"mmsi"}} },
                     "module_text_id": { "function":{"name":"create_olc_composite_key", "args":{"mmsi", "olc"}} } } ],

    config_filepath = '/srv/dogger/',
    config_filename = 'conf_cloud_db.ini')

udp_upload_ais.run()
