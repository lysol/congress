import sys
from time import sleep
import shlex, subprocess
from random import randint


rels = (
    (0, 1),
    (0, 2),
    (0, 3),
    (1, 4),
    (1, 5),
    (1, 6),
    (2, 7),
    (2, 8),
    (2, 9),
    (9, 4),
    (7, 6)
    )

first = [0, 1, 2, 3, 16, 17, 18, 19, 20,
    36, 37, 38, 39, 40, 41, 42]
rels = (
    (4, 0),
    (5, 0),
    (6, 0),
    (7, 1),
    (8, 1),
    (9, 1),
    (10, 2),
    (11, 2),
    (12, 2),
    (13, 3),
    (14, 3),
    (15, 3),
    (1, 0),
    (2, 1),
    (3, 0),
    (21, 16),
    (22, 16),
    (23, 16),
    (24, 17),
    (25, 17),
    (26, 17),
    (27, 18),
    (28, 18),
    (29, 18),
    (30, 19),
    (31, 19),
    (32, 19),
    (33, 20),
    (34, 20),
    (35, 20),
    (20, 3),
    (19, 2),
    (18, 5),
    (17, 7),
    (16, 10),
    (43, 36),
    (44, 36),
    (45, 36),
    (46, 37),
    (47, 37),
    (48, 37),
    (49, 38),
    (50, 38),
    (51, 38),
    (52, 39),
    (53, 39),
    (54, 39),
    (55, 40),
    (56, 40),
    (57, 40),
    (58, 41),
    (59, 41),
    (60, 41),
    (61, 42),
    (62, 42),
    (63, 42),
    (37, 36),
    (38, 37),
    (39, 38),
    (40, 39),
    (41, 40),
    (42, 41),
    (42, 20),
    (40, 18),
    (38, 16)
    )

connectors = range(64)

threads = []
try:
    for t in connectors:
        i = t
        matching = filter(lambda tc: tc[0] == t, rels)
        print 'Starting node %d' % i

        cmd = 'python2.6 -m congress.congress --debug --port %d --ctl %d ' % (19000 + i, 29000 + i)
        for match in matching:
            peer_port = match[1]
            cmd += ' --peer 127.0.0.1 %d' % (19000 + peer_port)
        args = shlex.split(cmd)
        p = subprocess.Popen(args)
        threads.append(p)
        sleep(3.0)
    while True:
        sleep(1)
except KeyboardInterrupt:
    for p in threads:
        p.kill()
