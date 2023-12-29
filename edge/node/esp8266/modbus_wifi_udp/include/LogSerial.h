#ifndef _LOGSERIAL_h
#define _LOGSERIAL_h

#define LOGLEVEL 1

namespace Bus::SerialBus::Hard
{
  class Log
  {
    public:
      #ifdef LOGLEVEL
      Log(int a_baud_rate = 9600, int a_log_level = LOGLEVEL);
      void write(int a_logged_int, int a_log_level = LOGLEVEL);
      void write(char a_logged_char, int a_log_level = LOGLEVEL);
      void write(const char* a_logged_string, int a_log_level = LOGLEVEL);
      void write(String a_logged_string, int a_log_level = LOGLEVEL);
      void write_hex(int a_logged_int, int a_log_level = LOGLEVEL);
      void linefeed(int a_log_level = LOGLEVEL);
      void writeln(int a_logged_int, int a_log_level = LOGLEVEL);
      void writeln(char a_logged_char, int a_log_level = LOGLEVEL);
      void writeln(const char* a_logged_string, int a_log_level = LOGLEVEL);
      void writeln(String a_logged_string, int a_log_level = LOGLEVEL);
      void writeln_hex(int a_logged_int, int a_log_level = LOGLEVEL);
      #else
      Log(...);
      void write(...);
      void write_hex(...);
      void linefeed(...);
      void writeln(...);
      void writeln_hex(...);
      #endif
    private:
      #ifdef LOGLEVEL
      int m_baud_rate;
      int m_log_level = LOGLEVEL;
      #endif
  };
}

#endif
