import glob
import numpy as np
from dateutil import parser

HEADER_SIZE = 4096

HEADER_PARSER = {
    "FILE_SIZE":int,
    "UTC_START":parser.parse,
    "NBIT":int,
    "NDIM":int,
    "NPOL":int,
    "NCHAN":int
}

class DadaFileStream(object):
    def __init__(self, files):
        self._files = sorted(files)
        self._read_header()

    def _read_header(self):
        self._header = {}
        with open(self._files[0],"r") as f:
            header = f.read(HEADER_SIZE)
        header = header.splitlines()
        for line in header:
            try:
                key = line.split()[0]
                if key in HEADER_PARSER.keys():
                    self._header[key] = HEADER_PARSER[key](line.split()[1])
            except:
                pass

    def extract(self, start, count):
        nsamps_per_file = self._header["FILE_SIZE"]/self._header["NBIT"]/self._header["NDIM"]/self._header["NPOL"]
        start_file = start/nsamps_per_file
        print "Start file:",start_file
        start_offset = start - (start_file * nsamps_per_file)
        print "Start file offset:", start_offset
        n = [nsamps_per_file-start_offset]
        all_data = []
        while sum(n) < count:
            n.append(min(nsamps_per_file,count-n))
        print "Read sizes:",n
        for fname,nsamps in zip(self._files[start_file:start_file+len(n)],n):
            print "Reading {} samples from {}".format(nsamps,fname)
            with open(fname,"r") as f:
                f.seek(HEADER_SIZE)
                data = np.fromfile(f,count=nsamps*1024,dtype='byte')
                data = data.astype("float32").view("complex64")
                all_data.append(data)
        data = np.array(all_data).ravel()
        return data.reshape(data.size/512,256,2)


def find_all(filestem):
    return sorted(glob.glob(filestem+"*"))