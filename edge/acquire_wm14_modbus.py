# import time
# import struct
#
# import gateway.io as io
#
#
# modbus_data = { 'offset': 1, 'max_chunk_size': 12,
# 'field_data': { 'VL1N': {'address': 0, 'type': 'float', 'value': None},
#                 'VL2N': {'address': 2, 'type': 'float', 'value': None},
#                 'VL3N': {'address': 4, 'type': 'float', 'value': None},
#                 'VL12': {'address': 6, 'type': 'float', 'value': None},
#                 'VL23': {'address': 8, 'type': 'float', 'value': None},
#                 'VL31': {'address': 10, 'type': 'float', 'value': None},
#                 'IL1': {'address': 12, 'type': 'float', 'value': None},
#                 'IL2': {'address': 14, 'type': 'float', 'value': None},
#                 'IL3': {'address': 16, 'type': 'float', 'value': None},
#                 'IN': {'address': 18, 'type': 'float', 'value': None},
#                 'P1': {'address': 20, 'type': 'float', 'value': None},
#                 'P2': {'address': 22, 'type': 'float', 'value': None},
#                 'P3': {'address': 24, 'type': 'float', 'value': None},
#                 'S1': {'address': 26, 'type': 'float', 'value': None},
#                 'S2': {'address': 28, 'type': 'float', 'value': None},
#                 'S3': {'address': 30, 'type': 'float', 'value': None},
#                 'Q1': {'address': 32, 'type': 'float', 'value': None},
#                 'Q2': {'address': 34, 'type': 'float', 'value': None},
#                 'Q3': {'address': 36, 'type': 'float', 'value': None},
#                 'PhSeq': {'address': 38, 'type': 'float', 'value': None},
#                 'PF1': {'address': 40, 'type': 'float', 'value': None},
#                 'PF2': {'address': 42, 'type': 'float', 'value': None},
#                 'PF3': {'address': 44, 'type': 'float', 'value': None},
#                 'VLN': {'address': 46, 'type': 'float', 'value': None},
#                 'VLL': {'address': 48, 'type': 'float', 'value': None},
#                 'P': {'address': 50, 'type': 'float', 'value': None},
#                 'S': {'address': 52, 'type': 'float', 'value': None},
#                 'Q': {'address': 54, 'type': 'float', 'value': None},
#                 'PF': {'address': 56, 'type': 'float', 'value': None},
#                 'Hz': {'address': 58, 'type': 'float', 'value': None} } }
#
# offset = 0
# offset = modbus_data['offset']
#
# max_chunk_size = 2
# max_chunk_size = modbus_data['max_chunk_size']
#
# max_address = 58
#
#
# modbus_instrument = io.ModbusSerial(host_port = '1-2', slaveaddress = 1, mode = io.ModbusSerial.MODE_RTU) #, baudrate = 19200)
#
# while True :
#
#     registers_int_list = modbus_instrument.read_registers_in_chunks(offset, max_chunk_size, max_address)
#
#     print("registers_int_list", registers_int_list)
#
#     registers_per_value = [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]
#
#     register_values = []
#     register_index = 0
#     for no_of_value_registers in registers_per_value :
#         format_string = "<" + "H" * no_of_value_registers
#         #print("format_string", format_string)
#         value_int_list = registers_int_list[register_index:register_index+no_of_value_registers]
#         value_int_list.reverse()
#         print("value_int_list", value_int_list)
#         unpacked_float = -9999.0
#         if len(value_int_list) == 2 :
#             packed_string = struct.pack(format_string, *value_int_list)
#             unpacked_float = struct.unpack("f", packed_string)[0]
#         register_values.append(unpacked_float)
#         register_index += no_of_value_registers
#
#     print("register_values", register_values)
#
#     time.sleep(1.0)


#field_data = modbus_data['field_data']
#selected_field_labels = ['VLN', 'VLL', 'P', 'S', 'Q', 'PF', 'Hz', 'IL1', 'IL2', 'IL3', 'IN', 'VL1N', 'VL2N', 'VL3N', 'VL12', 'VL23', 'VL31'] #'W L1', 'W L2', 'W L3', 'VA L1', 'VA L2', 'VA L3', 'VAr L1', 'VAr L2', 'VAr L3', 'Ph seq', 'PF L1', 'PF L2', 'PF L3']
#for selected_field_label in selected_field_labels :
#    selected_address_type_value = field_data[selected_field_label]
#    address = selected_address_type_value['address']
#    selected_address_type_value.update( {'value': round(float_value, 2)} )
#    field_data.update({selected_field_label: selected_address_type_value})
#output = {}
#for selected_field_label in selected_field_labels :
#    selected_address_type_value = field_data[selected_field_label]
#    value = selected_address_type_value['value']
#    print(' '*0 + '|'*0, (selected_field_label.ljust(0)).rjust(10), re.sub(r'\.?0+$',lambda match: ' '*(match.end()-match.start()),'{:>7.3f}'.format(value)),'|'*0)
#    output.update({selected_field_label: value})
#output_json_string = json.dumps(output)


import gateway.daqc

modbus = gateway.daqc.RegistersModbusSerialFile(
    channels = { 'WM14':{155:'txt'} },
    ctrl_channels = {},
    start_delay = 10,
    sample_rate = 1,
    host_port = '1-2',
    serial_baudrate = 19200,
    serial_parity = gateway.daqc.Serial.PARITY_NONE,
    serial_bytesize = gateway.daqc.Serial.EIGHTBITS,
    serial_stopbits = gateway.daqc.Serial.STOPBITS_ONE,
    modbus_slave_address = 1,
    modbus_mode = gateway.daqc.ModbusSerial.MODE_RTU,
    modbus_register_address_offset = 1,
    modbus_max_read_chunk_size = 12,
    modbus_max_read_address = 58,
    file_path = '/home/scc01/Z/data/files/',
    ctrl_file_path = '/home/scc01/Z/data/files/others/',
    archive_file_path = '/home/scc01/Z/data/files/',
    config_filepath = '/home/scc01/Z/app/python/dogger/',
    config_filename = 'conf.ini')

modbus.run()
