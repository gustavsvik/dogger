#ifndef _LOGSERIAL_h
#define _LOGSERIAL_h

namespace Bus::HardSerial
{
  class Log
  {
    public:
      Log(int a_baud_rate, int a_log_level = 1);
      void write(int a_logged_int, int a_log_level = 1);
      void write(char a_logged_char, int a_log_level = 1);
      void write(const char* a_logged_string, int a_log_level = 1);
      void write(String a_logged_string, int a_log_level = 1);
      void write_hex(int a_logged_int, int a_log_level = 1);
      void linefeed(int a_log_level = 1);
      void writeln(int a_logged_int, int a_log_level = 1);
      void writeln(char a_logged_char, int a_log_level = 1);
      void writeln(const char* a_logged_string, int a_log_level = 1);
      void writeln(String a_logged_string, int a_log_level = 1);
      void writeln_hex(int a_logged_int, int a_log_level = 1);

    private:
      int m_baud_rate;
      int m_log_level;
  };
}

#endif
