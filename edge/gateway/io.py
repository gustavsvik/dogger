import time
from typing import Union

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
            self.bus_port = Serial.search_port(self.host_port)



class Serial(Bus, serial.Serial) :


    def __init__(self,
        bus_port:           Union[ None, str ] = None,
        host_port:          Union[ None, str ] = None,
        baudrate:           Union[ None, int ] = 9600,
        parity:             Union[ None, str ] = serial.PARITY_NONE,
        bytesize:           Union[ None, int ] = serial.EIGHTBITS,
        stopbits:           Union[ None, int ] = serial.STOPBITS_ONE,
        timeout:            Union[ None, float ] = None,
        write_timeout:      Union[ None, float ] = None,
        xonxoff:            Union[ None, bool ] = False,
        rtscts:             Union[ None, bool ] = False,
        dsrdtr:             Union[ None, bool ] = False,
        inter_byte_timeout: Union[ None, float ] = None,
        exclusive:          Union[ None, bool ] = None,
        config_filepath:    Union[ None, str ] = None,
        config_filename:    Union[ None, str ] = None) :

        Bus.__init__(self)
        serial.Serial.__init__(self, port = None)

        self.host_port = host_port
        self.bus_port = bus_port

        self._port = bus_port
        self._baudrate = baudrate
        self._parity = parity
        self._bytesize = bytesize
        self._stopbits = stopbits
        self._timeout = timeout
        self._write_timeout = write_timeout
        self._xonxoff = xonxoff
        self._rtscts = rtscts
        self._dsrdtr = dsrdtr
        self._inter_byte_timeout = inter_byte_timeout
        self._exclusive = exclusive

        self.config_filepath = config_filepath
        self.config_filename = config_filename


    def init_serial(self) :

        try :
            self.open()
            time.sleep(0.1)
            if (self.isOpen()):
                print("connected to : " + self.portstr)
        except serial.serialutil.SerialException as e:
            rt.logging.exception(e)


    @staticmethod
    def search_port(host_port) :

        bus_port = None
        comport_list = serial.tools.list_ports.comports()
        for comport in comport_list:
            port_string = str(comport.device) + ';' + str(comport.name) + ';' + str(comport.description) + ';' + str(comport.hwid) + ';' + str(comport.vid) + ';' + str(comport.pid) + ';' + str(comport.serial_number) + ';' + str(comport.location) + ';' + str(comport.manufacturer) + ';' + str(comport.product) + ';' + str(comport.interface)
            print("port_string", port_string)
            print("host_port", host_port)
            if (port_string.find(host_port)) >= 0:
                bus_port = comport.device
                print("bus_port", bus_port)
        if bus_port is None : raise NoPortIdException("Neither bus nor host port ID (physical connector/slot) given.")
        return bus_port



class RS485(Serial) :

    pass



class SerialModbus(Bus) :


    MODE_RTU = minimalmodbus.MODE_RTU
    MODE_ASCII = minimalmodbus.MODE_ASCII


    def __init__(self,
        bus_port:        Union[ None, str ] = None,
        host_port:       Union[ None, str ] = None,
        baudrate:        Union[ None, int ] = 19200,
        parity:          Union[ None, str ] = serial.PARITY_NONE,
        bytesize:        Union[ None, int ] = serial.EIGHTBITS,
        stopbits:        Union[ None, int ] = serial.STOPBITS_ONE,
        timeout:         Union[ None, float ] = 0.2,
        write_timeout:   Union[ None, float ] = None,
        slaveaddress:    Union[ None, int ] = 1,
        mode:            Union[ None, Literal[minimalmodbus.MODE_RTU, minimalmodbus.MODE_ASCII] ] = minimalmodbus.MODE_RTU,
        ignore_checksum: Union[ None, bool ] = False,
        clear_buffers_before_each_transaction: Union[ None, bool ] = True,
        config_filepath: Union[ None, str ] = None,
        config_filename: Union[ None, str ] = None) :

        self.host_port = host_port
        self.bus_port = bus_port

        Bus.__init__(self)

        self.slaveaddress = slaveaddress
        self.mode = mode
        self.ignore_checksum = ignore_checksum
        self.clear_buffers_before_each_transaction = clear_buffers_before_each_transaction

        self.baudrate = baudrate
        self.parity = parity
        self.bytesize = bytesize
        self.stopbits = stopbits
        self.timeout = timeout
        self.write_timeout = write_timeout

        self.config_filepath = config_filepath
        self.config_filename = config_filename

        self.instrument = minimalmodbus.Instrument(self.bus_port, self.slaveaddress)
        self.instrument.serial.baudrate = self.baudrate
        self.instrument.serial.bytesize = self.bytesize
        self.instrument.serial.parity   = self.parity
        self.instrument.serial.stopbits = self.stopbits
        self.instrument.serial.timeout  = self.timeout
        self.instrument.mode = self.mode
        self.instrument.clear_buffers_before_each_transaction = self.clear_buffers_before_each_transaction


    def read_registers_in_chunks(self, offset, max_chunk_size, max_address) :

        no_of_whole_chunks = max_address // max_chunk_size
        end_of_whole_chunks = no_of_whole_chunks * max_chunk_size
        last_chunk_size = max_address - end_of_whole_chunks

        raw_values = []

        try :
            for chunk_no in range(0, no_of_whole_chunks) :
                start_address = chunk_no * max_chunk_size
                #print("start_address + offset", start_address + offset, "max_chunk_size", max_chunk_size)
                registers = self.instrument.read_registers(registeraddress = start_address + offset, functioncode = 3, number_of_registers = max_chunk_size)
                #print("registers", registers)
                raw_values += registers #[6:-4]
                #print("end_of_whole_chunks + offset", end_of_whole_chunks + offset, "last_chunk_size", last_chunk_size)
            last_registers = self.instrument.read_registers(registeraddress = end_of_whole_chunks + offset, functioncode = 3, number_of_registers = last_chunk_size + 2)
            #print("last_registers", last_registers)
            raw_values += last_registers #[6:-4]
        except (minimalmodbus.NoResponseError, minimalmodbus.InvalidResponseError) as e:
            print(e)

        return raw_values


    @staticmethod
    def search_port(host_port) :
        return Serial.search_port(host_port)

    #@staticmethod
    #def hexstring_to_float(hexstring: str, number_of_registers: int = 2) -> float :
    #
    #    byte_string = minimalmodbus._hexdecode(hexstring)
    #    byte_float = minimalmodbus._bytestring_to_float(bytestring = byte_string, number_of_registers = number_of_registers, byteorder = minimalmodbus.BYTEORDER_BIG)
    #    return byte_float


class NoPortIdException(Exception) :

    pass
