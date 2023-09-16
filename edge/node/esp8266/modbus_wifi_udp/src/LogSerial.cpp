#include "Arduino.h"

#include <LogSerial.h>


Bus::HardSerial::Log::Log(int a_baud_rate, int a_log_level) : m_baud_rate(a_baud_rate), m_log_level(a_log_level)
{
  Serial.begin(this->m_baud_rate);
}

void Bus::HardSerial::Log::write(int a_logged_int, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.print(a_logged_int);
}

void Bus::HardSerial::Log::write(const char* a_logged_string, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.print(a_logged_string);
}

void Bus::HardSerial::Log::write(char a_logged_char, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.print(a_logged_char);
}

void Bus::HardSerial::Log::write(String a_logged_string, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.print(a_logged_string);
}

void Bus::HardSerial::Log::write_hex(int a_logged_int, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.print(a_logged_int, HEX);
}

void Bus::HardSerial::Log::linefeed(int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.println();
}

void Bus::HardSerial::Log::writeln(int a_logged_int, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.println(a_logged_int);
}

void Bus::HardSerial::Log::writeln(const char* a_logged_string, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.println(a_logged_string);
}

void Bus::HardSerial::Log::writeln(char a_logged_char, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.println(a_logged_char);
}

void Bus::HardSerial::Log::writeln(String a_logged_string, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.println(a_logged_string);
}

void Bus::HardSerial::Log::writeln_hex(int a_logged_int, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.println(a_logged_int, HEX);
}
