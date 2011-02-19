from congress import *
import pyev
import sys
from time import sleep
import shlex, subprocess

if len(sys.argv) == 2:
    num_nodes = int(sys.argv[1])
else:
    num_nodes = 10

threads = []
try:
    for i in range(num_nodes):
        print 'Starting node %d' % i

        cmd = 'python congress.py --debug --port %d --ctl %d ' % (19000 + i, 29000 + i)
        if i > 0:
            cmd += ' --peer 127.0.0.1 %d' % (19000 + i - 1)
        args = shlex.split(cmd)
        p = subprocess.Popen(args)
        threads.append(p)
        sleep(0.3)
    while True:
        sleep(1)
except KeyboardInterrupt:
    for p in threads:
        p.kill()
