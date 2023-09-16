#ifndef _MODBUSSERIAL_h
#define _MODBUSSERIAL_h

#include <ModbusRTU.h>

class ModbusSoftSerial
{
  public:
    ModbusSoftSerial(int a_slave_id);
    int slaveId();

  private:
    int m_slave_id;
    bool errorCallback(Modbus::ResultCode event, uint16_t transaction_id, void* data);
};

#endif
