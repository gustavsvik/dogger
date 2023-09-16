#ifndef _CONSTANTS_h
#define _CONSTANTS_h

namespace Constants::Dimension
{
  const uint16_t MARIEX_BUFFERSIZE = 16;
  const uint16_t MIN_CHANNEL_INDEX = 255 + 1;
}

namespace Constants::Time
{
  const uint32_t INSTALL_TIMESTAMP = 1669500000;
  const uint32_t INVALID_TIMESTAMP = 4294967295;
  const uint32_t CYCLE_TIME = 5;
  const uint32_t CYCLES_BETWEEN_NTP = 200;
}

namespace Constants::Value
{
  const float INVALID_FLOAT_VALUE = -9999.9;
}

#endif
