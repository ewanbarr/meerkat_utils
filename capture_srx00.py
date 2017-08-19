from subprocess import Popen, PIPE
import os

FENG2DADA = "/home/pulsar/soft/psrdada_cpp/build/psrdada_cpp/meerkat/tools/feng2dada"
UDPDB = "/home/pulsar/soft/psrdada.20150805/asterix/udpdb"
HEADER_MAKER = "/home/pulsar/scripts/freq_calc.py"
HEADER = "/tmp/header.txt"

def make_dada_key_string(key):
    return "DADA INFO:\nkey {0}".format(key)

def reset_dada_buffers():
    os.system("dada_db -d")
    os.system("dada_db -n 20 -b 209715200 -l -p")
    os.system("dada_db -d -k caca")
    os.system("dada_db -k caca -n 20 -b 209715200 -l -p")

def make_header(group_id, filter_id):
    cmd = "python {} -a 4 -g {} -f {} -d > {}".format(
        HEADER_MAKER, group_id, filter_id, HEADER)
    os.system(cmd)

def capture(group_id, filter_id, out_path, tobs):
    group = "239.2.1.{}".format(150+group_id)
    reset_dada_buffers()
    make_header(group_id, filter_id)
    cmd = "dada_dbdisk -D {} -k caca".format(out_path)
    dada_dbdisk = Popen([cmd], stdout=PIPE, stderr=PIPE, shell=True)
    cmd = "{} -i dada -o caca -c 256".format(FENG2DADA)
    feng2dada = Popen([cmd], stdout=PIPE, stderr=PIPE, shell=True)
    cmd = "LD_PRELOAD=libvma.so {} -s {} -p 7148 -m {} -H {}".format(
        UDPDB, tobs, group, HEADER)
    os.system(cmd)
    dada_dbdisk.kill()
    feng2dada.kill()





