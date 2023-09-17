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

  const uint16_t REG_COUNT_MAX = 100;

  // ABB A44 energy meter
  const uint16_t SLAVE_ID = 1;
  const uint16_t MAX_N_REGS_PER_BLOCK = 28;

  /*
  // PID controller Red Lion PXU defaults
  const uint16_t SLAVE_ID = 247;
  const uint16_t MAX_N_REGS_PER_BLOCK = 100;
  const uint16_t WRITE_REG_COUNT = 2;
  */

  uint16_t TOTAL_REG_COUNT = 0;
}


#endif
