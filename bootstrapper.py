import sys
from time import sleep
import shlex, subprocess
from random import randint

if len(sys.argv) == 2:
    num_nodes = int(sys.argv[1])
else:
    num_nodes = 10

threads = []
try:
    for i in range(num_nodes):
        print 'Starting node %d' % i

        cmd = 'python -m congress.congress --port %d --ctl %d ' % (19000 + i, 29000 + i)
        if i > 0:
            peer_port = randint(0,i)
            cmd += ' --peer 127.0.0.1 %d' % (19000 + peer_port)
        args = shlex.split(cmd)
        p = subprocess.Popen(args)
        threads.append(p)
        sleep(0.3)
    while True:
        sleep(1)
except KeyboardInterrupt:
    for p in threads:
        p.kill()
