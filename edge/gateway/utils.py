

def nmea_checksum(nmea_data) :

    nmea_bytearray = bytes(nmea_data, encoding='utf8')
    checksum = 0
    for i in range(0, len(nmea_bytearray)) :
        if nmea_bytearray[i] != 44 :
            checksum = checksum ^ nmea_bytearray[i]
    checksum_hex = hex(checksum)
    return checksum_hex[2:]


def get_channel_range_string(channels) :

    return ';;'.join([str(ch) for ch in channels]) + ';;'
