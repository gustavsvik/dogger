import gateway.daqc


usb_serial = gateway.daqc.SerialNmeaFile(
    channels = { 'MMB':{142:'txt',143:'npy'} }, 
    start_delay = 10, 
    sample_rate = 1,
    location = '1-2',
    baudrate = 4800, 
    timeout = 0.01, 
    parity = 'E',
    stopbits = 1,
    bytesize = 7,
    file_path = '/home/heta/Z/data/files/', 
    archive_file_path = '/home/heta/Z/data/files/',
    start_pos = 1,
    delay_waiting_check = 0.01,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf.ini')
    
usb_serial.run()
