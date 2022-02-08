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
    print(f"""{name}: Working for {tmax} seconds...""")
    print(f"""{name}: Maximum memory is {memmax} GB""")
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
    while True:
        val = random.gauss(mu, sig)
        sum = sum + val
        memuse = proc.memory_info().rss/gb
        if memuse < memmax:
            for val1 in vals:
                val2s.append(val1*val)
            vals.append(val)
            memuse = proc.memory_info().rss/gb
            #with open(fnam, "wb") as fout:
            #    pickle.dump(vals, fout)
            #    pickle.dump(val2s, fout)
            #with open(fnam, "w") as fout:
            #    for val in vals:
            #        fout.write(f"{val}\n")
            #    for val in val2s:
            #        fout.write(f"{val}\n")
        n = n + 1
        mempak = 0
        tim = time.process_time() - t0
        if tim > tmax:
            fnam = f"out/{name}.out"
            print(f"""{name}: Writing {fnam} at {tim:.3f} sec""")
            pout1 = proc.io_counters().write_chars
            sout1 = psutil.disk_io_counters().write_bytes
            nout1 = psutil.net_io_counters().bytes_sent
            with open(fnam, "wb") as fout:
                print(f""" Start  pack at {time.time()}""")
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
            dpout = proc.io_counters().write_chars - pout1
            dsout = psutil.disk_io_counters().write_bytes - sout1
            dnout = psutil.net_io_counters().bytes_sent - nout1
            print(f"""{name}: Process, disk, network out: {dpout/gb:.3f}, {dsout/gb:.3f}, {dnout/gb:.3f} GB""")
            break
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
