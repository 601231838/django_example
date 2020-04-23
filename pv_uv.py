import os, sys, json, re, time
from itertools import *
from collections import *
import datetime
import pandas as pd
import numpy as np
from pandas import DataFrame, Series
from concurrent.futures import ProcessPoolExecutor, as_completed


def processPV(path):
    ct = Counter()
    with open(path) as fd:
        while True:
            line = fd.readline()
            if not line:
                break
            g = re.search('\d+\/[a-zA-Z]+\/\d+:\d+:\d+:\d+\s\+0800', line)
            tm = datetime.datetime.strptime(g.group(), '%d/%b/%Y:%H:%M:%S +0800')
            key = '_'.join([str(tm.year), str(tm.month), str(tm.day), str(tm.hour), str(tm.minute), str(tm.second)])
            ct[key] += 1
    print('the most pv list is :')
    for it in ct.most_common(5):
        print("\t\t %s==>%s" % (it[0], it[1]))
    print('the all pv is :')
    p = np.array(ct.values())
    print("\t\t", p.sum())
    del ct


def processUV(path):
    ct = Counter()
    ds = {}
    with open(path) as fd:
        while True:
            line = fd.readline()
            if not line:
                break
            gt = re.search('\d+\/[a-zA-Z]+\/\d+:\d+:\d+:\d+\s\+0800', line)
            tm = datetime.datetime.strptime(gt.group(), '%d/%b/%Y:%H:%M:%S +0800')
            key = '_'.join([str(tm.year), str(tm.month), str(tm.day), str(tm.hour), str(tm.minute), str(tm.second)])
            gd = re.search('(?<=deviceid=)\d+', line)
            if gd is not None:
                deviceid = gd.group()
                if deviceid not in ds:
                    ds[deviceid] = key
                    ct[key] += 1
            else:
                gg = re.search('(?<=deviceId=)\d+', line)
                if gg is not None:
                    deviceid = gg.group()
                    if deviceid not in ds:
                        ds[deviceid] = key
                        ct[key] += 1
    print('the most uv list is :')
    for it in ct.most_common(5):
        print("\t\t %s==>%s" % (it[0], it[1]))
    print('the all uv is :')
    print('\t\t', ds.__len__())
    del ds
    del ct


def test1(path):
    processPV(path)
    processUV(path)


def test2(path):
    with ProcessPoolExecutor(max_workers=4) as executor:
        tasks = [executor.submit(i, path) for i in [processPV, processUV]]
        for i in as_completed(tasks):
            print(f"in the test2 is running {i.running()} is done {i.done()}")


if __name__ == '__main__':
    if sys.argv.__len__() != 2:
        print('need only file path ')
    else:
        path = sys.argv[1]
    # path = "/home/ada/Desktop/familyshare.txt"
    old = time.time()
    test2(path)
    now = time.time()
    print(f"in the main process ok used time is {now-old}")

