import time
from typing import Union, Literal

try : import serial
except ImportError: pass
try : import serial.tools.list_ports
except ImportError: pass
try : import minimalmodbus
except ImportError: pass

import gateway.runtime as rt



class Bus :


    def __init__(self) :

        if self.bus_port is None :
            self.bus_port = self.search_port(self.host_port)


    @staticmethod
    def search_port(host_port) :

        pass



class Serial(Bus, serial.Serial) :


    (PARITY_NONE, PARITY_EVEN, PARITY_ODD, PARITY_MARK, PARITY_SPACE) = PARITIES = serial.Serial.PARITIES
    (FIVEBITS, SIXBITS, SEVENBITS, EIGHTBITS) = BYTESIZES = serial.Serial.BYTESIZES
    (STOPBITS_ONE, STOPBITS_ONE_POINT_FIVE, STOPBITS_TWO) = STOPBITS = serial.Serial.STOPBITS


    def __init__(self,
        bus_port:            Union[ str, None ] = None,
        host_port:           Union[ str, None ] = None,
        baudrate:            Union[ int, None ] = 9600,
        parity:              Union[ Literal[PARITY_NONE, PARITY_EVEN, PARITY_ODD, PARITY_MARK, PARITY_SPACE], None ] = PARITY_NONE,
        bytesize:            Union[ Literal[FIVEBITS, SIXBITS, SEVENBITS, EIGHTBITS], None ] = EIGHTBITS,
        stopbits:            Union[ Literal[STOPBITS_ONE, STOPBITS_ONE_POINT_FIVE, STOPBITS_TWO], None ] = STOPBITS_ONE,
        timeout:             Union[ float, None ] = None,
        write_timeout:       Union[ float, None ] = None,
        xonxoff:             Union[ bool, None ] = False,
        rtscts:              Union[ bool, None ] = False,
        dsrdtr:              Union[ bool, None ] = False,
        inter_byte_timeout:  Union[ float, None ] = None,
        exclusive:           Union[ bool, None ] = None,
        delay_waiting_check: Union[ float, None ] = 0.0,
        config_filepath:     Union[ str, None ] = None,
        config_filename:     Union[ str, None ] = None) :

        self.host_port = host_port
        self.bus_port = bus_port

        Bus.__init__(self)

        serial.Serial.__init__(self, port = None)

        self.port = self.bus_port

        self.baudrate = baudrate
        self.parity = parity
        self.bytesize = bytesize
        self.stopbits = stopbits
        self.timeout = timeout
        self.write_timeout = write_timeout
        self.xonxoff = xonxoff
        self.rtscts = rtscts
        self.dsrdtr = dsrdtr
        self.inter_byte_timeout = inter_byte_timeout
        self.exclusive = exclusive

        self.delay_waiting_check = delay_waiting_check

        self.config_filepath = config_filepath
        self.config_filename = config_filename


    def open_connection(self) :

        try :
            self.open()
            time.sleep(0.1)
            if (self.isOpen()) :
                rt.logging.debug("connected to : " + self.portstr)
        except serial.serialutil.SerialException as e:
            rt.logging.exception(e)


    def connection_is_open(self) :

        return self.isOpen()


    def read_string_response(self) :

        response_string = ''

        response = ''
        while self.in_waiting:
            response = self.read()
            try:
                response_string += response.decode()
            except UnicodeDecodeError as e:
                rt.logging.exception(e)

            time.sleep(self.delay_waiting_check)

        return response_string


    def close_connection(self) :
        self.close()


    @staticmethod
    def search_port(host_port) :

        bus_port = None
        comport_list = serial.tools.list_ports.comports()
        for comport in comport_list:
            port_string = str(comport.device) + ';' + str(comport.name) + ';' + str(comport.description) + ';' + str(comport.hwid) + ';' + str(comport.vid) + ';' + str(comport.pid) + ';' + str(comport.serial_number) + ';' + str(comport.location) + ';' + str(comport.manufacturer) + ';' + str(comport.product) + ';' + str(comport.interface)
            rt.logging.debug("port_string", port_string, "host_port", host_port)
            if (port_string.find(host_port)) >= 0:
                bus_port = comport.device
                rt.logging.debug("bus_port", bus_port)
        if bus_port is None : raise NoPortIdException("Neither bus nor host port ID (physical connector/slot) given.")
        return bus_port



class RS485(Serial) :

    pass



class ModbusSerial(Bus) :


    (MODE_RTU, MODE_ASCII) = MODES = (minimalmodbus.MODE_RTU, minimalmodbus.MODE_ASCII)

    (PARITY_NONE, PARITY_EVEN, PARITY_ODD, PARITY_MARK, PARITY_SPACE) = PARITIES = Serial.PARITIES
    (FIVEBITS, SIXBITS, SEVENBITS, EIGHTBITS) = BYTESIZES = Serial.BYTESIZES
    (STOPBITS_ONE, STOPBITS_ONE_POINT_FIVE, STOPBITS_TWO) = STOPBITS = Serial.STOPBITS


    def __init__(self,
        bus_port:        Union[ str, None ] = None,
        host_port:       Union[ str, None ] = None,
        baudrate:        Union[ int, None ] = 19200,
        parity:          Union[ Literal[PARITY_NONE, PARITY_EVEN, PARITY_ODD, PARITY_MARK, PARITY_SPACE], None ] = PARITY_NONE,
        bytesize:        Union[ Literal[FIVEBITS, SIXBITS, SEVENBITS, EIGHTBITS], None ] = EIGHTBITS,
        stopbits:        Union[ Literal[STOPBITS_ONE, STOPBITS_ONE_POINT_FIVE, STOPBITS_TWO], None ] = STOPBITS_ONE,
        timeout:         Union[ float, None ] = None,
        write_timeout:   Union[ float, None ] = None,
        slaveaddress:    Union[ int, None ] = 1,
        mode:            Union[ Literal[MODE_RTU, MODE_ASCII], None ] = MODE_RTU,
        config_filepath: Union[ str, None ] = None,
        config_filename: Union[ str, None ] = None) :

        self.host_port = host_port
        self.bus_port = bus_port

        Bus.__init__(self)

        self.slaveaddress = slaveaddress
        self.mode = mode

        self.baudrate = baudrate
        self.parity = parity
        self.bytesize = bytesize
        self.stopbits = stopbits
        self.timeout = timeout
        self.write_timeout = write_timeout

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        self.instrument = minimalmodbus.Instrument(self.bus_port, self.slaveaddress)

        self.instrument.mode = self.mode
        self.instrument.serial.baudrate = self.baudrate
        self.instrument.serial.parity   = self.parity
        self.instrument.serial.bytesize = self.bytesize
        self.instrument.serial.stopbits = self.stopbits
        self.instrument.serial.timeout  = self.timeout
        self.instrument.serial.write_timeout  = self.write_timeout


    def read_registers_in_chunks(self, slave_address, offset, max_chunk_size, max_address) :

        self.instrument.address = slave_address
        rt.logging.debug("self.instrument", self.instrument)

        no_of_whole_chunks = max_address // max_chunk_size
        end_of_whole_chunks = no_of_whole_chunks * max_chunk_size
        last_chunk_size = max_address - end_of_whole_chunks

        raw_values = []

        try :
            for chunk_no in range(0, no_of_whole_chunks) :
                start_address = chunk_no * max_chunk_size
                rt.logging.debug("start_address + offset", start_address + offset, "max_chunk_size", max_chunk_size)
                registers = self.instrument.read_registers(registeraddress = start_address + offset, functioncode = 3, number_of_registers = max_chunk_size)
                rt.logging.debug("registers", registers)
                raw_values += registers #[6:-4]
                rt.logging.debug("end_of_whole_chunks + offset", end_of_whole_chunks + offset, "last_chunk_size", last_chunk_size)
            last_registers = self.instrument.read_registers(registeraddress = end_of_whole_chunks + offset, functioncode = 3, number_of_registers = last_chunk_size + 2)
            rt.logging.debug("last_registers", last_registers)
            raw_values += last_registers #[6:-4]
        except (minimalmodbus.NoResponseError, minimalmodbus.InvalidResponseError) as e:
            rt.logging.exception(e)

        return raw_values


    @staticmethod
    def search_port(host_port) :
        return Serial.search_port(host_port)



class NoPortIdException(Exception) :

    pass
