import logging
import logging.handlers
import os
import sys



class LogRecord(logging.LogRecord):
    def getMessage(self):
        msg = self.msg
        if self.args:
            if isinstance(self.args, dict):
                msg = msg.format(**self.args)
            else:
                msg = msg.format(*self.args)
        return msg

        
        
class Logger(logging.Logger):
    def makeRecord(self, name, level, fn, lno, msg, args, exc_info, func=None, extra=None):
        rv = LogRecord(name, level, fn, lno, msg, args, exc_info, func)
        if extra is not None:
            for key in extra:
                rv.__dict__[key] = extra[key]
        return rv



runtime_filepath = os.path.dirname(__file__) + '/logs/'
runtime_log_file = 'logging.log'

if runtime_filepath is not None and os.path.exists(runtime_filepath):
    runtime_log_file = runtime_filepath + runtime_log_file


logging.setLoggerClass(Logger)
logging.basicConfig(filename = runtime_log_file, level = logging.ERROR)
should_roll_over = os.path.isfile(runtime_log_file)

try:
    handler = logging.handlers.RotatingFileHandler(runtime_log_file, mode = 'w', backupCount = 20)
    if should_roll_over:  # log already exists, roll over!
        handler.doRollover()
except (PermissionError, OSError) as e:
    print(e)
