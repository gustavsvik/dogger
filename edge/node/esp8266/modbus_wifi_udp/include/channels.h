#ifndef _CHANNELS_h
#define _CHANNELS_h


namespace Channels::Dimension
{
}

namespace Channels::Config
{
  const uint16_t COUNT = 3;

  struct REG_BLOCK
  {
    int16_t FIRST;
    int16_t COUNT;
  };

  const REG_BLOCK REG_BLOCKS[Channels::Config::COUNT] = 
  {
      {0, 0}
    , {0, 0}
    , {23296, 28} // 0x5B00 instantaneous values starting address
    //, {20480, 12} // 0x5000 accumulator values starting address
  };

  /*
  // PID controller Red Lion PXU defaults
  const uint16_t COUNT = 3;
  const REG_BLOCK REG_BLOCKS[Channels::Dimension::COUNT] = 
  {
      {65535, 65535}
    , {65535, 65535}
    , {0, 14}
  };
  const uint16_t WRITE_REG_COUNT = 2;
  */
}

#endif
