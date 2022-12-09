import gateway.link


udp_upload_ais = gateway.link.SqlUdpAivdmPosition(
    channels = {168,169},
    start_delay = 0,
    transmit_rate = 1,
    port = 4444,
    max_age = 10,
    mmsi = 123456789,
    vessel_name = "TSB TEST AREA PATROL",
    call_sign = "7SA9998",
    ship_type = 55,
    nav_status = 0)

udp_upload_ais.run()
