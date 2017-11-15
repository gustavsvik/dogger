from nordpool import elspot
from pprint import pprint
from numpy import save
import time
import datetime
import math
import requests
import http
import socket
from dogger.metadata import Configure

#FILE_PATH = "../../data/files/"
#FILE_PATH = "C:/Z/THISBUSINESS/Energilab/PROJECTS/logging/data/files/"

config = Configure()
FILE_PATH = config.getDataFilePath()


while True:

    prices = elspot.Prices()
    tomorrows_date = datetime.date.today() + datetime.timedelta(days=1)
    
    hourly_structure = []
    hourly_values = []
    try:
        hourly_structure = prices.hourly(end_date=tomorrows_date, areas=['SE2'])
        pprint(hourly_structure)
        hourly_values = hourly_structure['areas']['SE2']['values']
        no_of_hourly_values = len(hourly_values)
    except (requests.exceptions.Timeout, requests.exceptions.TooManyRedirects, requests.exceptions.RequestException, requests.exceptions.ConnectionError, socket.gaierror, http.client.IncompleteRead, ConnectionResetError, requests.packages.urllib3.exceptions.ProtocolError) as e:
        print(e)

    for current_value in hourly_values:
        value = current_value['value']
        print(value)
        if not math.isinf(value):
            start_time = current_value['start'].timestamp()
            print(start_time)
            #end_time = current_value['end'].timestamp()
            #print(end_time)
            try:
                filename_spotpower = repr(500) + "_" + repr(int(start_time))
                save(FILE_PATH+filename_spotpower, [float(value)])
            except PermissionError as e:
                print(e)


    time.sleep(1800.0)
