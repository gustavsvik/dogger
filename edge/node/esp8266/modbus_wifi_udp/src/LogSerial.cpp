#include "Arduino.h"

#include <LogSerial.h>


#ifdef LOGLEVEL

Bus::SerialBus::Hard::Log::Log(int a_baud_rate, int a_log_level) : m_baud_rate(a_baud_rate), m_log_level(a_log_level)
{
  Serial.begin(this->m_baud_rate);
}

void Bus::SerialBus::Hard::Log::write(int a_logged_int, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.print(a_logged_int);
}

void Bus::SerialBus::Hard::Log::write(const char* a_logged_string, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.print(a_logged_string);
}

void Bus::SerialBus::Hard::Log::write(char a_logged_char, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.print(a_logged_char);
}

void Bus::SerialBus::Hard::Log::write(String a_logged_string, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.print(a_logged_string);
}

void Bus::SerialBus::Hard::Log::write_hex(int a_logged_int, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.print(a_logged_int, HEX);
}

void Bus::SerialBus::Hard::Log::linefeed(int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.println();
}

void Bus::SerialBus::Hard::Log::writeln(int a_logged_int, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.println(a_logged_int);
}

void Bus::SerialBus::Hard::Log::writeln(const char* a_logged_string, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.println(a_logged_string);
}

void Bus::SerialBus::Hard::Log::writeln(char a_logged_char, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.println(a_logged_char);
}

void Bus::SerialBus::Hard::Log::writeln(String a_logged_string, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.println(a_logged_string);
}

void Bus::SerialBus::Hard::Log::writeln_hex(int a_logged_int, int a_log_level) 
{ 
  if (a_log_level >= this->m_log_level) Serial.println(a_logged_int, HEX);
}

#else

Bus::SerialBus::Hard::Log::Log(...) {}
void Bus::SerialBus::Hard::Log::write(...) {}
void Bus::SerialBus::Hard::Log::write_hex(...) {}
void Bus::SerialBus::Hard::Log::linefeed(...) {}
void Bus::SerialBus::Hard::Log::writeln(...) {}
void Bus::SerialBus::Hard::Log::writeln_hex(...) {}

#endif