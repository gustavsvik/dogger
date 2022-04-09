import gateway.daqc


usb_serial = gateway.daqc.SerialNmeaFile(
    channels = { 'MMB':{142:'txt',143:'npy'} },
    start_delay = 10,
    sample_rate = 1,
    host_port = '1-2',
    serial_baudrate = 4800,
    serial_parity = gateway.daqc.Serial.PARITY_EVEN,
    serial_bytesize = gateway.daqc.Serial.SEVENBITS,
    serial_stopbits = gateway.daqc.Serial.STOPBITS_ONE,
    serial_timeout = 0.01,
    delay_waiting_check = 0.01,
    file_path = '/home/heta/Z/data/files/',
    archive_file_path = '/home/heta/Z/data/files/',
    config_filepath = '/home/heta/Z/app/python/dogger/',
    config_filename = 'conf.ini')

usb_serial.run()
