from subprocess import Popen, PIPE
import os, atexit

HEADER_MAKER = "/home/pulsar/scripts/freq_calc.py"
HEADER = "/tmp/header.txt"

UDP2DB = ("nvidia-docker run -d "
    "--net=host "
    "--ulimit memlock=-1 "
    "--device=/dev/infiniband/rdma_cm "
    "--device=/dev/infiniband/uverbs0 "
    "--device=/dev/infiniband/uverbs1 "
    "-v /tmp/:/tmp/ "
    "--ipc=host "
    "--name udp2db "
    "--rm "
    "-e LD_PRELOAD=libvma.so "
    "srx00:5000/dspsr:cuda8.0 "
    "udp2db -s {tobs} -p 7148 -m {group} -H /tmp/header.txt")

FENG2DADA = ("nvidia-docker run "
    "--ulimit memlock=-1 "
    "--ipc=host "
    "--name feng2dada "
    "--rm "
    "srx00:5000/dspsr:cuda8.0 "
    "feng2dada -i dada -o caca -c 256 --log_level=debug")

DADADBDISK = ("nvidia-docker run -d "
    "--ulimit memlock=-1 "
    "--ipc=host "
    "--name dadadbdisk "
    "--rm "
    "-v {output}:/output/ "
    "srx00:5000/dspsr:cuda8.0 "
    "dada_dbdisk -k caca -D /output/")

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
    os.system(DADADBDISK.format(output=out_path))
    os.system(FENG2DADA)
    os.system(UDP2DB.format(tobs=tobs,group=group))
    os.system("docker kill feng2dada")
    os.system("docker kill dadadbdisk")

def cycle_capture(filter_id, tobs, base_path):
    for group_id in range(16):
        out_path = os.path.join(base_path,"group_%02d"%(group_id))
        try:
            os.mkdir(out_path)
        except Exception as error:
            print error
        capture(group_id, filter_id, out_path, tobs)

