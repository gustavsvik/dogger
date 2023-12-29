#ifndef _DEVICES_h
#define _DEVICES_h

#include "SoftwareSerial.h"

/*
namespace Bus::SerialBus
{
  class Soft
  {
    public :
      Soft(uint16_t a_baud_rate, uint8_t a_rx_pin = 4, uint8_t a_tx_pin = 5);
      void begin();
      static const uint16_t BAUD_RATE = 9600;

    private :
      uint16_t m_baud_rate;
      uint8_t m_rx_pin;
      uint8_t m_tx_pin;

      SoftwareSerial m_softwareserial;

      //const uint16_t BYTE_LENGTH = 8;
      //const char PARITY_BITS = 'N';
      //const uint16_t STOP_BITS = 1;
  };
}

namespace Bus
{
  class Modbus : SoftSerial
  {
    public:
      Modbus(int a_baud_rate, int a_log_level = 1);
*/
namespace Bus::SerialBus::Modbus
{
  const uint16_t BAUD_RATE = 9600;
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
