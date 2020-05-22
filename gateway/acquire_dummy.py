import time
import math
import numpy
import shutil
import base64


samples_per_chan = 1
subsamples_per_chan = 1
sample_rate = 1
module_chan_range = [1]
text_chan_index_start = [65533]
unique_chan_index_start = [65534]
image_chan_index = [65535]
data_filepath = '/home/heta/Z/data/files/'
global_error_code = 0
img_data = b'iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAMAAABEpIrGAAAABGdBTUEAALGPC/xhBQAAACBjSFJNAAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAACalBMVEX////+/v7U1NSDg4KTk5OcnJzk5OTt7e2GhoVkZGN3d3dtbWydnZz09PT4+Piurq2GhoZ/f35mZmWKioqOjo3IyMjf39+fn597e3pgYF9MTEt1dXSUlJO6urn7+/u5ubmCgoKYmJiRkZCUlJSkpKTw8PDz8/OgoKCRkZGPj49ra2uDg4Oenp7v7+90dHRmZmaXl5ZycnKKionr6+vp6emOjo58fHyBgYCWlpWBgYGVlZWoqKfo6OiTk5J2dnWrq6uAgIDKysl8fHtHR0ZISEeSkpKhoaHq6uqLi4tHR0UmJiWWlpbOzs719fWSkpFpaWhqamlJSUhcXFvi4uIZGRdWVlU6OjkuLizc3NvHx8YlJSMtLStubm41NTMwMC7b29tCQkF+fn1jY2Jra2rl5eXx8fGqqqqlpaWurq6wsLCsrKzm5uaJiYh0dHNZWVhMTEq0tLP9/f3V1dRVVVVVVVNNTUtDQ0JPT06trazc3NxwcHCMjIx9fXxycnFvb254eHfn5+eEhIN7e3tpaWlzc3Kfn575+fl1dXV6ennExMTR0dH8/PyFhYSzs7P39/fu7u63t7eCgoGHh4aMjIuYmJfPz8/6+vvy8vK9vb2mpqaioqGjo6OlpaTAwMDs7OzNzc1jY2NsbGyAgH8sLCqenp2FhYWHh4fExMMzMzEeHhwgIB4zMzJubm2ZmZmEhIShoaBnZ2ZEREJdXVxKSklSUlGIiIh5eXjY2NhiYmFoaGdISEZ6enrW1tbFxcVnZ2c+Pj0jIyFxcXCNjY2ZmZj29vbKysqQkJCdnZ2mpqXCwsL6+vrY2NewsK+ioqK6+bTuAAAAAWJLR0QAiAUdSAAAAAlwSFlzAAASdAAAEnQB3mYfeAAAAAd0SU1FB+QEEQwWBqDFjIgAAAHeSURBVDjLY2AAAkYGJmYWVjYG3ICdg5OLm4cXpzwfv4CgkLCIKE4FYuISklLSMrJyOOQZ5RVkFBWVlFVUcShQU9cQ0GTR0tbRxaFAlVVP30DY0EjdGIcCE1MzcwtNSytrXN6wsZWws3dQcHDE5QljdSdnF3NXN3dcCtSsPTy9tL19GHEp8PXzDwiUDApmwKWC0SEkVD8sPAKnAobIqOiY2Lh4nCHNIJaQkJiUnIJbQWpaekZmljFuBdk5uUJ5+QW4pAsLi4pLSsvKK3C5sZChsqq6prauPhWnFQ2NSk3NLa1qOBW0KdcqcbR34E5yDJ3M9drtXbjlGbp9ejJ7+6Dhil2FWKWJajdYvptRLlW1n5eXEVlp/4SJ2pMmT5nK18Y4bXrEjJmzZtvPmTuPEaFi/oKmhYtyF1dYBAWYiy8xZVW2WiqzjGM5woQMae9JOU4rMvNWrlo9SalHZ03tWqV1tUHZcAXruTmcGjdsFNfi2lS+eYulsMZW5W1BrhzbEW7Q0dtqLrVBZcuOnbuiopZWaxjpJO6emLsHYce8vRZK+zSy9occOMghuXNri92hwxxrHPjg8kDnyh1pYJ931C134jHhpcLCCluPnziJlNEQ/jl1uvPM2uPKZ/cyQbUDAJGUf71qdyJEAAAAJXRFWHRkYXRlOmNyZWF0ZQAyMDIwLTA0LTE3VDEyOjIyOjA2KzAyOjAw97iYVgAAACV0RVh0ZGF0ZTptb2RpZnkAMjAyMC0wNC0xN1QxMjoyMjowNiswMjowMIblIOoAAABXelRYdFJhdyBwcm9maWxlIHR5cGUgaXB0YwAAeJzj8gwIcVYoKMpPy8xJ5VIAAyMLLmMLEyMTS5MUAxMgRIA0w2QDI7NUIMvY1MjEzMQcxAfLgEigSi4A6hcRdPJCNZUAAAAASUVORK5CYII='


def read_samples():

    data = 1.23456789*numpy.ones(100,dtype=numpy.float64)
    return data


def downsample(y, size):

    y_reshape = y.reshape(size, int(len(y)/size))
    y_downsamp = y_reshape.mean(axis=1)
    return y_downsamp


def loop_acquire():

    capture_filename_img = 'image_' + repr(image_chan_index[0]) + '.png'
    with open(capture_filename_img, "wb") as fh:
        fh.write(base64.decodebytes(img_data))    

    while (global_error_code >= 0):

        sec_array = None
        try:
            sec_array = read_samples()
        except OSError as e:
            print(e)

        acq_finish_time = numpy.float64(time.time())
        acq_finish_secs = numpy.int64(acq_finish_time)
        acq_finish_microsecs = numpy.int64(acq_finish_time * 1e6)
        acq_microsec_part = acq_finish_microsecs - numpy.int64(acq_finish_secs) * 1e6

        filename_img = repr(image_chan_index[0]) + '_' + repr(acq_finish_secs) + '.' + 'png'
        try:
            shutil.copy(capture_filename_img, data_filepath + filename_img)
        except (FileNotFoundError, PermissionError) as e:
            print(e)

        no_of_chans = len(module_chan_range)
        
        for chan_index in range(0, no_of_chans):

            numpy_filename = repr(unique_chan_index_start[chan_index]) + "_" + repr(acq_finish_secs)
            sec_value = downsample(sec_array[samples_per_chan*(chan_index+0):samples_per_chan*(chan_index+1)], 1)
            sec_array = numpy.concatenate(([0.0], acq_microsec_part, sec_array), axis = None)
            sec_array[0] = sec_value
            try:
                pass
                numpy.save(data_filepath + numpy_filename, sec_array)
            except PermissionError as e:
                print(e)
                
            text_filename = repr(text_chan_index_start[chan_index]) + "_" + repr(acq_finish_secs)
            numpy.savetxt(data_filepath + text_filename + '.' + 'csv', sec_array, delimiter=",")

            #sec_array_string = numpy.array2string(sec_array, separator = ',', max_line_width = numpy.inf, formatter = {'float': lambda x: format(x, '6.5E')})
            #try:
            #    f = open(data_filepath + filename + '.csv', 'wb')
            #    f.write(bytes(sec_array_string[1:-1], 'utf-8'))
            #    f.close
            #except PermissionError as e:
            #    print(e)


        time.sleep(0.5)


loop_acquire()
