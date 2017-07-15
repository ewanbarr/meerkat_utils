import struct
import pcapy
import socket
import io
import numpy as np
import ctypes
import os
import sys
import jinja2
from astropy.time import Time
from collections import deque

descriptor_map = {
    1:"heap_counter",
    2:"heap_size",
    3:"heap_offset",
    4:"payload_size",
    5632:"timestamp",
    16641:"feng_id",
    16643:"frequency",
    17152:"feng_raw"
}


FULL_DTYPE = [
    ("dest_mac_addr","|S17"),
    ("src_mac_addr","|S17"),
    ("frame_length","uint32"),
    ("udp_length","uint32"),
    ("src_addr","|S15"),
    ("dest_addr","|S15"),
    ("src_port","uint32"),
    ("dest_port","uint32"),
    ("length","uint32"),
    ("checksum","uint32"),
    ("magic_number","ubyte"),
    ("version","ubyte"),
    ("item_pointer_width","ubyte"),
    ("heap_addr_width","ubyte"),
    ("reserved","uint32"),
    ("num_items","uint32"),
    ("heap_counter","uint32"),
    ("heap_size","uint32"),
    ("heap_offset","uint32"),
    ("payload_size","uint32"),
    ("timestamp","uint32"),
    ("feng_id","uint32"),
    ("frequency","uint32"),
    ("feng_raw","int32"),
    ("data","complex64",(256,2))
]


TICKS_PER_HEAP = 2097152
NSAMPS_PER_PACKET = 256
NPOL = 2

DADA_HEADER = """
HEADER       DADA                # Distributed aquisition and data analysis
HDR_VERSION  1.0                 # Version of this ASCII header
HDR_SIZE     4096                # Size of the header in bytes

DADA_VERSION 1.0                 # Version of the DADA Software
PIC_VERSION  1.0                 # Version of the PIC FPGA Software

# DADA parameters
OBS_ID       {{obs_id}}          # observation ID
PRIMARY      unset               # primary node host name
SECONDARY    unset               # secondary node host name
FILE_NAME    unset               # full path of the data file

FILE_SIZE    {{filesize}}  # requested file size
FILE_NUMBER  0             # number of data file

# time of the rising edge of the first time sample
UTC_START    {{utc_start}}               # yyyy-mm-dd-hh:mm:ss.fs
MJD_START    {{mjd}}            # MJD equivalent to the start UTC

OBS_OFFSET   0                   # bytes offset from the start MJD/UTC
OBS_OVERLAP  0                   # bytes by which neighbouring files overlap

# description of the source
SOURCE {{source_name}}        # source name
RA     {{ra}}                 # RA of source
DEC    {{dec}}                # DEC of source

# description of the instrument
TELESCOPE    {{telescope}}     # telescope name
INSTRUMENT   {{instrument}}    # instrument name
RECEIVER     {{receiver_name}} # Receiver name
FREQ         {{frequency_mhz}} # observation frequency
BW           {{bandwidth}}     # bandwidth in MHz
TSAMP        {{tsamp}}         # sampling interval in microseconds
BYTES_PER_SECOND {{bytes_per_second}}

NBIT         {{nbit}}                   # number of bits per sample
NDIM         {{ndim}}                   # dimension of samples (2=complex, 1=real)
NPOL         {{npol}}                   # number of polarizations observed
NCHAN        {{nchan}}                  # number of channels here
RESOLUTION   {{resolution}}             # a parameter that is unclear
DSB          {{dsb}}
# end of header
"""

DADA_DEFAULTS = {
    "obs_id": "unset",
    "filesize": 2500000000,
    "mjd": 55555.55555,
    "source": "B1937+21",
    "ra": "00:00:00.00",
    "dec": "00:00:00.00",
    "telescope": "MeerKAT",
    "instrument": "feng",
    "receiver_name": "lband",
    "frequency_mhz": 1260,
    "bandwidth": 16,
    "tsamp": 0.0625,
    "nbit": 8,
    "ndim": 2,
    "npol": 2,
    "nchan": 1,
    "resolution":1,
    "dsb":0
}

def dada_defaults():
    return DADA_DEFAULTS.copy()

def render_dada_header(overrides):
    defaults = DADA_DEFAULTS.copy()
    defaults.update(overrides)
    bytes_per_second = defaults["bandwidth"] * 1e6 * \
        defaults["nchan"] * 2 * defaults["npol"] * defaults["nbit"] / 8
    defaults.update({
        "bytes_per_second": bytes_per_second,
    })
    return jinja2.Template(DADA_HEADER).render(**defaults)

class Descriptor(ctypes.BigEndianStructure):
    _fields_ = [
    ("is_value", ctypes.c_uint8,1),
    ("id",ctypes.c_uint32,15),
    ("value",ctypes.c_uint64,48)
    ]

class DescriptorUnion(ctypes.Union):
    _fields_ = [
    ("struct",Descriptor),
    ("uint64",ctypes.c_uint64)
    ]

def format_mac_address(mac_string):
    return ':'.join('%02x' % b for b in bytearray(mac_string))

def parse_descriptor_id(desc_id):
    return descriptor_map.get(desc_id,"unknown")

def read_ethII_header(stream):
    header = {}
    header["dest_mac_addr"] = format_mac_address(stream.read(6))
    header["src_mac_addr"] = format_mac_address(stream.read(6))
    header["frame_length"] = struct.unpack("!H",stream.read(2))[0]
    return header

def read_ipv4_header(stream):
    header = {}
    stream.read(2)
    header["udp_length"] = struct.unpack("!H",stream.read(2))[0]
    stream.read(8)
    header["src_addr"] = socket.inet_ntoa(stream.read(4))
    header["dest_addr"] = socket.inet_ntoa(stream.read(4))
    return header

def read_udp_header(stream):
    header = {}
    header["src_port"],header["dest_port"] = struct.unpack("!HH",stream.read(4))
    header["length"],header["checksum"] = struct.unpack("!HH",stream.read(4))
    return header

def read_spead_header(stream):
    header = {}
    header["magic_number"] =  struct.unpack("b",stream.read(1))[0]
    header["version"] =  struct.unpack("b",stream.read(1))[0]
    header["item_pointer_width"] =  struct.unpack("b",stream.read(1))[0]
    header["heap_addr_width"] =  struct.unpack("b",stream.read(1))[0]
    header["reserved"] =  struct.unpack("!H",stream.read(2))[0]
    header["num_items"] =  struct.unpack("!H",stream.read(2))[0]
    return header

def read_spead_descriptor(stream):
    u = DescriptorUnion()
    u.uint64 = struct.unpack("Q",stream.read(8))[0]
    descriptor_id = u.struct.id
    value_or_address = u.struct.value
    return {parse_descriptor_id(descriptor_id):value_or_address}

def read_spead_data(stream,n):
    return np.array(struct.unpack(n*"b",stream.read(n))).reshape(NSAMPS_PER_PACKET,NPOL,2)

def parse_packet(data):
    header = {}
    stream = io.BytesIO(data)
    header.update(read_ethII_header(stream))
    header.update(read_ipv4_header(stream))
    header.update(read_udp_header(stream))
    header.update(read_spead_header(stream))
    for item in range(header["num_items"]):
        header.update(read_spead_descriptor(stream))
    header["data"] = read_spead_data(stream,1024)
    stream.close()
    return header

def parse_packet_lite(data):
    #14 byte ethII header
    #20 byte ipv4 header
    #8 byte udp header
    #8 byte spead header
    stream = io.BytesIO(data)
    stream.seek(50)
    header = {}
    for item in range(11):
        header.update(read_spead_descriptor(stream))
    header["data"] = read_spead_data(stream,1024)
    stream.close()
    return header

def read_pcap_spead_stream(fname,npackets):
    packets = pcapy.open_offline(fname)
    data = []
    for _ in range(npackets):
        packet = packets.next()
        if packet[0] is not None:
            data.append(parse_packet(packet[1]))
    return data

class Heap(object):
    def __init__(self, nchans, nsamps, npol):
        self._data = np.zeros([nchans,nsamps,npol,2],dtype="byte")

    def add(self, packet):
        channel_id = packet["heap_offset"] / packet["payload_size"]
        self._data[channel_id,:,:,:] = packet['data']

    def reset(self):
        self._data[:] = 0

    def to_dada_order(self):
        return self._data.transpose(1,0,2,3)


class AntennaSubbandRingBuffer(object):
    def __init__(self, antenna_id, subband_id, opts):
        print "New AntennaSubbandRingBuffer instance created"
        print "Antenna:",antenna_id
        print "Subband:",subband_id
        self._first_pass = True
        self._nheaps = 3
        if opts.prefix is not None:
            filename = "%s_%02d_%05d.dada"%(opts.prefix,antenna_id,subband_id)
        else:
            filename = "%02d_%05d.dada"%(antenna_id,subband_id)
        self.file = open(filename,"w")
        print "Output filename:",filename

    def _write_header(self,packet):
        header = dada_defaults()
        header['nchan'] = packet['heap_size'] / packet['payload_size']
        chbw = opts.bandwidth/opts.nchan
        header['bandwidth'] = header['nchan'] * chbw
        header["npol"] = NPOL
        header["nbit"] = 8
        mid_channel = packet['frequency'] + header['nchan']/2
        header["frequency_mhz"] = chbw * mid_channel + opts.cfreq - opts.bandwidth/2
        header['tsamp'] = 1 / chbw
        t = Time(opts.global_sync_epoch,format="unix",scale="utc",precision=9)
        header['utc_start'] = t.iso.replace(" ","-")
        header['mjd'] = t.mjd
        self.file.write(render_dada_header(header))
        self.file.seek(4096)

    def flush(self):
        self._timestamps = [i+TICKS_PER_HEAP for i in self._timestamps]
        heap = self._heaps[0]
        heap.to_dada_order().tofile(self.file)
        heap.reset()
        self._heaps.rotate(-1)

    def add(self, packet):
        if self._first_pass:
            self._write_header(packet)
            nchans_in_subband = packet['heap_size']/packet['payload_size']
            self._timestamps = [packet['timestamp']+i*TICKS_PER_HEAP for i in range(self._nheaps)]
            self._heaps = deque([Heap(nchans_in_subband,NSAMPS_PER_PACKET,NPOL) for _ in range(self._nheaps)])
            self._first_pass = False
        try:
            heap_id = self._timestamps.index(packet['timestamp'])
        except ValueError as error:
            print "Packet is too far out of order to be used"
            return
        if heap_id == (self._nheaps-1):
            self.flush()
            heap_id -= 1
        self._heaps[heap_id].add(packet)

    def close(self):
        for heap in self._heaps:
            heap.to_dada_order().tofile(self.file)
        self.file.close()

class RingBufferManager(object):
    def __init__(self, opts):
        self._buffers = {}
        self._opts = opts

    def add(self,packet):
        ant = packet['feng_id']
        subband = packet['frequency']
        key = (ant,subband)
        if key not in self._buffers:
            self._buffers[key] = AntennaSubbandRingBuffer(ant,subband,self._opts)
        self._buffers[key].add(packet)

    def close_all(self):
        for key,rb in self._buffers.items():
            rb.close()

def stream_to_buffers(packet_generator, opts):
    rb_manager = RingBufferManager(opts)
    max_packets = opts.npackets
    packet_count = 0
    while True:
        packet = packet_generator.next()
        if packet[0] is None:
            break
        else:
            rb_manager.add(parse_packet_lite(packet[1]))
        packet_count+=1
        if max_packets is not None:
            if packet_count >= max_packets:
                break
    rb_manager.close_all()
    return rb_manager

def main(opts):
    packet_generator = pcapy.open_offline(opts.fname)
    return stream_to_buffers(packet_generator,opts)

if __name__ == "__main__":
    from argparse import ArgumentParser
    usage = "usage: {prog} [options]".format(prog=sys.argv[0])
    parser = ArgumentParser(usage=usage)
    required = parser.add_argument_group('required arguments')
    required.add_argument('-i','--fname', dest='fname', type=str,
        help='The name of the pcap file to read.')
    required.add_argument('-c','--nchan', dest='nchan', type=int,
        help='The number of F-engine channels, e.g. 4096')
    required.add_argument('-t','--global_sync_epoch', dest='global_sync_epoch', type=float,
        help='The unix time global synchronization epoch')
    optional = parser.add_argument_group('optional arguments')
    optional.add_argument('-p','--prefix', dest='prefix', type=str,
        default=None, help='A prefix for output filenames. The output format will be {prefix}_{feng_id}_{subband_id}.dada')
    optional.add_argument('-n','--npackets', dest='npackets', type=int,
        default=None, help='The number of packets to read. Default is to read all packets.')
    optional.add_argument('-b','--bandwidth', dest='bandwidth', type=float,
        default=856.0, help='The total bandwidth in MHz (default is 856 MHz).')
    optional.add_argument('-f','--centre_freq', dest='cfreq', type=float,
        default=1284.0, help='The centre frequency in MHz (default is 1284 MHz).')
    opts = parser.parse_args()
    main(opts)
