import logging
import logging.handlers
import os
import sys

import metadata


config = metadata.Configure()
env = config.get()


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

        
log_file = 'logging.log'

if env['STORE_PATH'] is not None and os.path.exists(env['STORE_PATH']):
    log_file = env['STORE_PATH'] + log_file
elif env['WINDOWS_STORE_PATH'] is not None and os.path.exists(env['WINDOWS_STORE_PATH']):
    log_file = env['WINDOWS_STORE_PATH'] + log_file

logging.setLoggerClass(Logger)
logging.basicConfig(filename=log_file,level=logging.ERROR)
should_roll_over = os.path.isfile(log_file)

try:
    handler = logging.handlers.RotatingFileHandler(log_file, mode='w', backupCount=5)
    if should_roll_over:  # log already exists, roll over!
        handler.doRollover()
except (PermissionError, OSError) as e:
    print(e)
