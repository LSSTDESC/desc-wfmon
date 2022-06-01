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

def cpuburn(name, tmax=0, memmax=0, nwfmax=0, nsam=1000, ngen=1, a_outdir=""):
    '''
    Test program to stress CPU, memory and I/O.
    Waveforms of fixed length are sequentially generated in a single thread.
    They are kept in memory until a memory limit is reached at which point
    they are packed, written to a file and cleared.
    The process continues until a specified count or time limit is reached.
    Each waveform includes nsam samples drawn from the mean of ngen calls
    to random.random().
    Parameters:
      name - label used a prifix for the output files
      tmax - Maximum run time [s]
      memmax - Memory limit [GB]
      nwfmax - Maximum number of waveforms to generate
      nsam - Number of samples in each waveform.
      ngen - Number of generator calls/sample
    
    Example performance
    -------------------
    nwfmax    nsam    ngen  Gins  Ttim[s]  Mem [GB]
       10k    100k       0  1905    146        3.80
      100k     10k       0  1904    137        3.80     730
      100k     10k       1  3540    252        3.80     400 (10k wf)/sec
      100k     10k       2  4700    337        3.80     300
    '''
    gb = 2**30
    proc = psutil.Process()
    vers = 'cpb-2.0.0'
    smemlim = 'unlimited'
    if memmax > 0: smemlim = f"{memmax} GB"
    stimlim = 'unlimited'
    if tmax > 0: stimm = f"{tmax} sec"
    outdir = 'out'
    if len(a_outdir):
        outdir = a_outdir
    print(f"{name}: Version {vers}")
    print(f"{name}: Label: {name}")
    print(f"{name}: Waveform nsam: {nsam}")
    print(f"{name}: Waveform ngen: {ngen}")
    print(f"{name}: Waveform limit: {nwfmax}")
    print(f"{name}:     Time limit: {stimlim}")
    print(f"{name}:   Memory limit: {smemlim}")
    print(f"{name}:  Output directory: {outdir}")
    print(f"{name}: Working directory: {os.getcwd()}")
    if tmax <= 0 and nwfmax <= 0:
        print(f"{name}: ERROR: Waveform count or time limit must be set.")
        return 1
    if nsam < 1:
        print(f"{name}: ERROR: Generated samples/wf must be > 0.")
        return 1
    if ngen < 0:
        print(f"{name}: ERROR: Waveform stride must must be >= 0.")
        return 1
    memsize = 0
    memuse = 0
    inpsize = 0
    outsize = 0
    gb = 2**30
    mb = 2**20
    t0 = time.process_time()
    print(f"""{name}: Starting memory is {proc.memory_info().rss/gb:.3f} GB""")
    nfil = 0
    wfos = []
    line = f"{name}: ---------------------------------------------------"
    nwf = 0
    nint = 0
    nwri = 0
    sumtimg = 0    # Time spent generating values.
    sumtimw = 0    # Time spent writing.
    timg = None
    memg = None
    vsum = 0
    iwf = 0
    while True:
        memuse = proc.memory_info().rss/gb
        tim = time.process_time() - t0
        jobdone = (tmax > 0 and tim > tmax) or (nwfmax > 0 and nwf >= nwfmax)
        filldone = memuse > memmax
        if filldone or jobdone:
            print(line)
            fnam = f"{outdir}/{name}-{nfil}.out"
            print(f"""{name}: Finished generating {nwf} waveforms at time {timg:.3f} sec, memory {memg:.3f} GB""")
            print(f"""{name}: Writing {fnam} at {tim:.3f} sec""")
            pout1 = proc.io_counters().write_chars
            sout1 = psutil.disk_io_counters().write_bytes
            nout1 = psutil.net_io_counters().bytes_sent
            with open(fnam, "wb") as fout:
                print(f"{name}:    Start pack {nfil} at time {tim:.3f} sec, memory {memuse:.3f} GB")
                print(f"{name}:    Waveform count is {len(wfos)}")
                for wfo in wfos:
                    s1 = struct.pack('f'*len(wfo), *wfo)
                    fout.write(s1)
                fout.close()
            dpout = proc.io_counters().write_chars - pout1
            dsout = psutil.disk_io_counters().write_bytes - sout1
            dnout = psutil.net_io_counters().bytes_sent - nout1
            timw = time.process_time() - t0
            sumtimw += timw - tim
            memw = proc.memory_info().rss/gb
            print(f"""{name}:   Finish write {nfil} at time {timw:.3f} sec, memory {memw:.3f} GB""")
            print(f"""{name}:   Process, disk, network out: {dpout/gb:.3f}, {dsout/gb:.3f}, {dnout/gb:.3f} GB""")
            nfil = nfil + 1
            if tim >= tmax: break
            wfos = []
            nwri = nwri + 1
            if jobdone: break
        else:
            wfo = array('f', [0.0]*nsam)
            # Use generator to evaluate on stride.
            isam = 0
            while isam < nsam:
                igen = 0
                val = 0.0
                if ngen > 0:
                    fac = 1/ngen
                    igen = 0
                    while igen < ngen:
                        val += random.random()
                        igen += 1
                wfo[isam] = val - 0.5
                isam += 1
            wfos.append(wfo)
            timg = time.process_time() - t0
            sumtimg += timg - tim
            nwf = nwf + 1
            memg = proc.memory_info().rss/gb
            memuse = memg
        iwf += 1
        mempak = 0
    print(line)
    dio = proc.io_counters()
    inpsize = dio.read_chars/gb
    outsize = dio.write_chars/gb
    memexit = proc.memory_info().rss/gb
    print(f"{name}: Number of wafeforms is {iwf}/{nwf}")
    print(f"{name}: Number of writes is {nwri}")
    print(f"{name}: Finish time is {time.process_time() - t0 :9.3f} sec")
    print(f"{name}:      Generate: {sumtimg:9.3f} sec")
    print(f"{name}:         Write: {sumtimw:9.3f} sec")
    print(f"{name}: Finish memory is {memuse:.3f} [pack is {mempak:.3f}, exit is {memexit:.3f}] GB")
    print(f"{name}: Finish  input is {inpsize:.3f} GB")
    print(f"{name}: Finish output is {outsize:.3f} GB")
    #time.sleep(15)
    outnap = dio.write_chars/gb
    memnap = proc.memory_info().rss/gb
    print(f"{name}: After-nap memory is {memnap:.3f} GB")
    print(f"{name}: After-nap output is {outnap:.3f} GB")

def main_cpuburn():
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} NAME TMAX MEMMAX NWFMAX NSAM NGEN [OUTDIR]")
        exit(0)
    name = sys.argv[1]
    tmax = float(sys.argv[2])
    memmax = float(sys.argv[3])
    nwfmax = int(sys.argv[4])
    nsam = int(sys.argv[5])
    ngen = int(sys.argv[6])
    outdir = '' if len(sys.argv) < 7 else sys.argv[7]
    cpuburn(name, tmax, memmax, nwfmax, nsam, ngen, outdir)
