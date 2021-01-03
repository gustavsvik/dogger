import gateway.daqc


usb_serial = gateway.daqc.SerialNmeaFile(
    channels = { 'VDM':{144:'txt'} , 'VDO':{145:'txt'} , 'ALV':{146:'txt'} , 'ALR':{147:'txt'} }, 
    start_delay = 10, 
    sample_rate = 1,
    location = '1-5',
    baudrate = 115200, 
    timeout = 0.01, 
    parity = 'E',
    stopbits = 1,
    bytesize = 8,
    file_path = '/home/heta/Z/data/files/', 
    archive_file_path = '/home/heta/Z/data/files/',
    start_pos = 1,
    delay_waiting_check = 0,
    config_filepath = '/home/heta/Z/app/python/dogger/', 
    config_filename = 'conf.ini')
    
usb_serial.run()
