#ifndef _DEVICES_h
#define _DEVICES_h

namespace Bus::SoftSerial
{
  const uint16_t BAUD_RATE = 9600;
  //const uint16_t BYTE_LENGTH = 8;
  //const char PARITY_BITS = 'N';
  //const uint16_t STOP_BITS = 1;
}

namespace Bus::SoftSerial::Modbus
{
  const uint16_t MAX_REG_COUNT = 28;
  // ABB A44 energy meter
  const uint16_t SLAVE_ID = 1;
  const uint16_t FIRST_REG = 23296; // 0x5B00 instantaneous values starting address
  const uint16_t REG_COUNT = 28;
  /*
  // PID regulator Red Lion PXU defaults
  const uint16_t SLAVE_ID = 247;
  const uint16_t FIRST_REG = 0;
  const uint16_t REG_COUNT = 14;
  const uint16_t WRITE_REG_COUNT = 2;
  */
}

#endif
