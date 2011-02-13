from clumsy import *
import random
import argparse

k = 20

PING = 1
PONG = 2
ID_REQUEST = 3
ID_REPLY = 4

def ping_handler(message, server, peer):
    reply = Message(PONG)
    peer.enqueue_message(reply)

def pong_handler(message, server, peer):
    self._hit_peer(peer)

def id_handler(message, server, peer):
    print 'Received ID for %s' % repr(peer.address)
    server.peers[server.peers.index(peer)].id = message.data['id']
    server._hit_peer(peer)

def id_requested(message, server, peer):
    print 'ID requested.'
    m = Message(ID_REPLY, data={'id': server.id})
    peer.enqueue_message(m)

def handshake_id(peer, server):
    id_request = Message(ID_REQUEST)
    peer.enqueue_message(id_request)


class CongressPeer(ClumsyPeer):

    def __init__(self, conn, addr, server):
        ClumsyPeer.__init__(self, conn, addr, server)
        self.id = None

class Congress(Clumsy):

    def _hit_peer(self, peer):
        if not peer.id and not peer.server_address:
            return
        dist = peer.id ^ self.id
        matching_buckets = filter(lambda (i, b): dist > 2**i and \
            dist < 2**(i+1), enumerate(self.buckets))
        for i, bucket in matching_buckets: 
            if peer in bucket:
                bucket.remove(peer)
                bucket.insert(0, peer)
            elif len(bucket) < 20:
                bucket.insert(0, peer)
            else:
                if peer in self.replacement_buckets[i]:
                    self.replacement_buckets[i].remove(peer)
                self.replacement_buckets[i].insert(0, peer)
                bucket[-1].enqueue_message(Message(PING))
        if self.debug:
            bs = filter(lambda (i, b): b != [], enumerate(self.buckets))
            rbs = filter(lambda (i, r): r != [],
                enumerate(self.replacement_buckets))
            if len(bs) > 0:
                print 'Buckets after hit: %s' % repr(bs)
            if len(rbs) > 0:
                print 'Repl. Buckets after hit: %s' % repr(rbs)

    def _make_buckets(self):
        self.buckets = []
        self.replacement_buckets = []
        for i in range(160):
            self.buckets.append([])
            self.replacement_buckets = []

    def _gen_id(self):
        self.id = random.getrandbits(160)

    def bootstrap_peer(self, conn):
        peer = self.add_peer(conn, connect=True)
        peer.enqueue_message(Message(ID_REQUEST))

    def __init__(self, host='0.0.0.0', port=19800, initial_peers=[],
        debug=False):
        Clumsy.__init__(self, (host, port), client_class=CongressPeer,
            debug=debug)
        self._gen_id()
        self._make_buckets()
        self.register_message_handler(PING, ping_handler)
        self.register_message_handler(PONG, pong_handler)
        self.register_message_handler(ID_REQUEST, id_requested)
        self.register_message_handler(ID_REPLY, id_handler)
        self.register_handshake(handshake_id)

        for conn in initial_peers:
            print 'Connecting to %s' % repr(conn)
            self.bootstrap_peer(conn)


def main():
    parser = argparse.ArgumentParser(description='Run a Congress instance.')
    parser.add_argument('-H', '--host', help='Local hostname',
        default='0.0.0.0')
    parser.add_argument('-P', '--port', help='Local port',
        default=19800, type=int)
    parser.add_argument('-p', '--peer', help='Add a peer in the form of '
        'HOST PORT', nargs=2, action='append')
    parser.add_argument('-d', '--debug', help='Turn on debug output.',
        action='store_true', default=False)
    args = parser.parse_args()

    if args.peer is not None:
        peer_conns = [(p[0], int(p[1])) for p in args.peer]
    else:
        peer_conns = []

    server = Congress(host=args.host, port=args.port, initial_peers=peer_conns,
        debug=args.debug)
    server.start()

if __name__ == "__main__":
    main()
