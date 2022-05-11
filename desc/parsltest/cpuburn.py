# cpuburn.py
#
# python script to burn CPU

import random
import time
import sys
import os
import psutil
import pickle
import struct
from array import array

def cpuburn(name, tmax, memmax):
    gb = 2**30
    proc = psutil.Process()
    mu = 0.0
    sig = 1.0
    n = 0
    sum = 0
    print(f"{name}: Working for {tmax} seconds...")
    print(f"{name}: Maximum memory is {memmax} GB")
    print(f"{name}: Working directory: {os.getcwd()}")
    memsize = 0
    memuse = 0
    inpsize = 0
    outsize = 0
    gb = 2**30
    mb = 2**20
    usearr = True
    if usearr:
        vals = array('f')
        val2s = array('f')
    else:
        vals = []
        val2s = []
    val3s = []
    val4s = []
    lines = []
    t0 = time.process_time()
    print(f"""{name}: Starting memory is {proc.memory_info().rss/gb:.3f} GB""")
    nfil = 0
    stride = 100
    nvalmax = 0
    line = f"{name}: ---------------------------------------------------"
    while True:
        memuse = proc.memory_info().rss/gb
        tim = time.process_time() - t0
        jobdone = tim > tmax
        if nvalmax > 0:
            filldone = len(vals) >= nvalmax
        else:
            filldone = memuse > memmax
        if filldone or jobdone:
            print(line)
            fnam = f"out/{name}-{nfil}.out"
            print(f"""{name}: Writing {fnam} at {tim:.3f} sec""")
            pout1 = proc.io_counters().write_chars
            sout1 = psutil.disk_io_counters().write_bytes
            nout1 = psutil.net_io_counters().bytes_sent
            with open(fnam, "wb") as fout:
                print(f"""{name}:    Start pack {nfil} at time {tim:.3f} sec, memory {memuse:.3f} GB""")
                print(f"""{name}:    Array lengths are {len(vals)//1000}k, {len(val2s)//1000}k""")
                s1 = struct.pack('f'*len(vals), *vals)
                fout.write(s1)
                blklen = 10000
                nblk = len(val2s)//blklen
                for iblk in range(nblk):
                    i1 = iblk*blklen
                    i2 = i1 + blklen
                    blk = val2s[i1:i2]
                    s2 = struct.pack('f'*len(blk), *blk)
                    mempak = max(mempak, proc.memory_info().rss/gb)
                    fout.write(s2)
                fout.close()
            timw = time.process_time() - t0
            memw = proc.memory_info().rss/gb
            print(f"""{name}:   Finish pack {nfil} at time {timw:.3f} sec, memory {memw:.3f} GB""")
            dpout = proc.io_counters().write_chars - pout1
            dsout = psutil.disk_io_counters().write_bytes - sout1
            dnout = psutil.net_io_counters().bytes_sent - nout1
            print(f"""{name}:   Process, disk, network out: {dpout/gb:.3f}, {dsout/gb:.3f}, {dnout/gb:.3f} GB""")
            nfil = nfil + 1
            if tim >= tmax: break
            nvalmax = len(vals)
            if usearr:
                vals = array('f')
                val2s = array('f')
            else:
                vals = []
                val2s = []
            if jobdone: break
        else:
            for s in range(0, stride):
                val = random.gauss(mu, sig)
                sum = sum + val
                vals.append(val)
                for val1 in vals:
                    val2s.append(val1*val)
            memuse = proc.memory_info().rss/gb
        n = n + 1
        mempak = 0
    print(line)
    mean = sum/n
    dio = proc.io_counters()
    inpsize = dio.read_chars/gb
    outsize = dio.write_chars/gb
    memexit = proc.memory_info().rss/gb
    len1 = len(vals)
    len2 = len(val2s)
    print(f"""{name}: Mean is {mean}""")
    print(f"""{name}: Number of samples is {n}""")
    print(f"""{name}: Array sizes are {len1} + {len2} = {len1+len2}""")
    print(f"""{name}: Finish time is {time.process_time() - t0 :.3f} sec""")
    print(f"""{name}: Finish memory is {memuse:.3f} [pack is {mempak:.3f}, exit is {memexit:.3f}] GB""")
    print(f"""{name}: Finish  input is {inpsize:.3f} GB""")
    print(f"""{name}: Finish output is {outsize:.3f} GB""")
    #time.sleep(15)
    outnap = dio.write_chars/gb
    memnap = proc.memory_info().rss/gb
    print(f"""{name}: After-nap memory is {memnap:.3f} GB""")
    print(f"""{name}: After-nap output is {outnap:.3f} GB""")

def main_cpuburn():
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} NAME TMAX MEMMAX")
        exit(0)
    name = sys.argv[1]
    tmax = float(sys.argv[2])
    memmax = float(sys.argv[3])
    cpuburn(name, tmax, memmax)
