import gateway.daqc


usb_serial = gateway.daqc.SerialNmeaFile(
    channels = { 'VDM':{144:'txt'} , 'VDO':{145:'txt', 146:'npy', 147:'npy'} , 'ALV':{150:'txt'} , 'ALR':{151:'txt'} },
    ctrl_channels = { 'GGA':{164:'txt'} },
    start_delay = 10,
    sample_rate = 1,
    host_port = '1-1',
    serial_baudrate = 115200,
    serial_parity = gateway.daqc.Serial.PARITY_EVEN,
    serial_stopbits = gateway.daqc.Serial.STOPBITS_ONE,
    serial_bytesize = gateway.daqc.Serial.SEVENBITS,
    serial_timeout = 0.01,
    delay_waiting_check = 0)

usb_serial.run()
