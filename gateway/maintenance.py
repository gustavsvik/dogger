#

import gateway.task as t



class NetworkTime:


    def __init__(self):
        pass


    def get_network_time(self):
    
        import datetime
        from contextlib import closing
        from socket import gaierror, socket, AF_INET, SOCK_DGRAM, SOCK_STREAM
        import struct, time, sys, datetime

        NTPSERVER, PORT = 'pool.ntp.org', 123

        
        try :

            with closing(socket(AF_INET, SOCK_DGRAM)) as s:
            
                t0 = time.time()

                s.sendto(('\x23' + 47 * '\0').encode(), (NTPSERVER, PORT))   # see https://stackoverflow.com/a/26938508
                msg, address = s.recvfrom(1024)                   # and https://stackoverflow.com/a/33436061
                unpacked = struct.unpack("!12I", msg[0:struct.calcsize("!12I")])  # ! => network (= big-endian), 12 => returns 12-tuple, I => unsigned int
                t3 = time.time()
                t1 = unpacked[8] + float(unpacked[9]) / 2**32 - 2208988800     # see https://tools.ietf.org/html/rfc5905#page-19
                t2 = unpacked[10] + float(unpacked[11]) / 2**32 - 2208988800   # and https://tools.ietf.org/html/rfc5905#page-13
                offset = ((t1 - t0) + (t2 - t3)) / 2    # https://en.wikipedia.org/wiki/Network_Time_Protocol#Clock_synchronization_algorithm
                roundtrip = (t3 -  t0) - (t2 - t1)

                print("Local computer time (t0)                               %.3f" % t0)
                print("NTP server time (t1, receive timestamp)                %.3f" % t1)
                print("NTP server time (t2, transmit timestamp)               %.3f" % t2)
                print("Local computer time (t3)                               %.3f" % t3)
                print("Offset                                                 %.1f ms" % (offset * 1000) )
                print("Local -> NTP server -> local roundtrip time            %.1f ms" % (roundtrip * 1000) )
                print("New local computer time                                %.3f" % (t3 + offset) )
        
                self.current_system_time = t3
                self.current_system_time_offset = offset

        except gaierror as e :
            print(e)

        dt = datetime.datetime.utcfromtimestamp(self.current_system_time + self.current_system_time_offset)

        return dt


    def adjust_system_time(self, dt):

        import win32api  # pip install pywin32

        win32api.SetSystemTime(dt.year, dt.month, dt.isocalendar()[2], dt.day, dt.hour, dt.minute, dt.second, int(dt.microsecond / 1000) )

        
    def run_continuous_adjustment(self, secs_interval):
    
        import time, numpy

        adj_time = self.get_network_time()
        self.adjust_system_time(adj_time)

        acq_prev_time = numpy.float64(time.time())
        
        diff_time = 0

        while True :

            acq_finish_time = numpy.float64(time.time())
            diff_time += acq_finish_time - acq_prev_time
            acq_prev_time = acq_finish_time

            if diff_time > secs_interval :
                adj_time = self.get_network_time()
                self.adjust_system_time(adj_time)
                diff_time = 0
