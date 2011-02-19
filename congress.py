from clumsy import *
import random
import argparse
import hashlib
import struct
import pyev
import socket
import traceback
import sys

k = 20
a = 3

PING = 1
PONG = 2
RPC_STORE = 5
RPC_FIND_NODE = 6
RPC_GET = 7
RPC_FIND_NODE_REPLY = 8
RPC_GET_REPLY = 9


def sha1_hash(key):
    sha1 = hashlib.sha1()
    sha1.update(key)
    new_id = long(sha1.digest().encode('hex'), 16)
    return new_id

def ping_handler(message, server, peer):
    reply = Message(PONG, re=message.id)
    peer.enqueue_message(reply)

def pong_handler(message, server, peer):
    if peer.active:
        server._hit_peer(peer)

#def id_handler(message, server, peer):
#    server._hit_peer(peer)

def handle_rpc_store(message, server, peer):
    for key in message.data['store'].keys():
        server.store[long(key)] = message.data['store'][key]

def handle_rpc_get(message, server, peer):
    if 'attempt' in message.data.keys():
        attempt = message.data['attempt']
    else:
        attempt = 1
    key = message.data['key']
    if key in server.store.keys():
        m = Message(RPC_GET_REPLY, 
            data={'store': {str(key): server.store[key]}, 'attempt': attempt},
            re=message.id)
        peer.enqueue_message(m)
    else:
        m = Message(RPC_GET_REPLY, data={'peers': [], 'key': key,
            'attempt': attempt}, re=message.id)
        closest = server._closest_peers(key, a)
        for other_peer in closest:
            m.data['peers'].append((other_peer.id,
                other_peer.server_address[0], other_peer.server_address[1]))
        peer.enqueue_message(m)

def value_check(message, server):
    for tup in list(server.retrieval_callbacks):
        (key, callback) = tup
        hash_key = sha1_hash(key)
        server._debug("Key: %s\nStore: %s" % (hash_key, server.store))
        if hash_key in [long(yek) for yek in server.store.keys()]:
            try:
                callback(key, message.data['store'][str(hash_key)])
            except Exception, e:
                traceback.print_exc(file=sys.stderr)
            server.retrieval_callbacks.remove(tup)
            return True
    return False

def handle_rpc_get_reply(message, server, peer):
    if 'attempt' in message.data.keys():
        attempt = message.data['attempt']
    else:
        attempt = 1
    # We actually got the value
    if 'peers' not in message.data.keys():
        # store it
        for (skey, sval) in message.data['store'].iteritems():
            server._debug('Storing value received from peer.')
            server.store[long(skey)] = sval
        if value_check(message, server):
            return
    # got a peer list instead
    else:
        if value_check(message, server):
            return
        if attempt > k:
            return
        for (node_id, address, port) in message.data['peers']:
            request = Message(RPC_GET,
                data={'key': message.data['key'], 'attempt': attempt + 1},
                re=message.id)
            if node_id not in [pee.id for pee in server.peers]:
                xpeer = server.bootstrap_peer((address, port), id=node_id)
                xpeer.enqueue_message(request)
            else:
                matched = filter(lambda pee: pee.id == node_id, server.peers)
                if len(matched) > 0:
                    matched[0].enqueue_message(request)


def handle_rpc_find_node(message, server, peer):
    new_id = message.data['node_id']
    closest = server._closest_peers(new_id, k)
    m = Message(RPC_FIND_NODE_REPLY,
        re=message.id,
        data={'peers': [(p.id, p.server_address[0], p.server_address[1]) for \
                p in closest]})
    peer.enqueue_message(m)

def handle_rpc_find_node_reply(message, server, peer):
    peer_tuples = message.data['peers']
    server_ids = [p.id for p in server.peers]
    for (node_id, address, port) in \
        filter(lambda e: e[0] not in server_ids and e[0] != server.id,
        peer_tuples):
        new_peer = server.bootstrap_peer((address, port), id=node_id)


class CongressPeer(ClumsyPeer):

    def __del__(self):
        ClumsyPeer.__del__(self)

    def __init__(self, conn, addr, server):
        ClumsyPeer.__init__(self, conn, addr, server)

class Congress(Clumsy):

    def _hit_peer(self, peer):
        if not peer.active:
            self._debug("Peer not active. What.")
            return
        if peer.id is None:
            self._debug("Peer ID None during _hit_peer.")
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
        #if self.debug:
        #    bs = filter(lambda (i, b): b != [], enumerate(self.buckets))
        #    rbs = filter(lambda (i, r): r != [],
        #        enumerate(self.replacement_buckets))

    def _node_id_present(self, node_id):
        for bucket in self.buckets:
            for peer in bucket:
                if type(peer.id) != long:
                    peer.id = long(peer.id)
                if peer.id == node_id:
                    return True
        return False

    def _conn_present(self, conn):
        if type(conn) != tuple:
            conn = tuple(conn)
        for bucket in self.buckets:
            for peer in bucket:
                if type(peer.address) != tuple:
                    peer.address = tuple(peer.address)
                if conn == peer.address:
                    return True
                if type(peer.server_address) != tuple:
                    peer.server_address = tuple(peer.server_address)
                if conn == peer.server_address:
                    return True
        return False

    def _make_buckets(self):
        self.buckets = []
        self.replacement_buckets = []
        for i in range(160):
            self.buckets.append([])
            self.replacement_buckets.append([])

    def _closest_peers(self, id, how_many):
        self._debug('Getting %d closest peers for %s' % (how_many, id))
        active_peers = filter(lambda p: p.active and \
            p.server_address is not None and \
            p.id is not None, self.peers)
        closest = sorted(active_peers, key=lambda peer: peer.id ^ id)
        closest = closest[:how_many]
        return closest

    def rpc_get(self, key, callback):
        """Since value retrieval is async, provide a callback that will handle
        the value."""
        new_id = sha1_hash(key)
        if new_id in self.store.keys():
            callback(key, self.store[new_id])
            self._debug('Fired a callback for key %s' % key)
            return True
        closest = self._closest_peers(new_id, a)
        self._debug('Fetched %d closest peers' % len(closest))
        for peer in closest:
            message = Message(RPC_GET, data={'key': new_id})
            self.register_message_callback(message, handle_rpc_get_reply)
            peer.enqueue_message(message)
        self.retrieval_callbacks.append((key, callback))

    def rpc_store(self, key, value):
        # This "works"
        new_id = sha1_hash(key)
        for peer in self._closest_peers(new_id, k):
            message = Message(RPC_STORE, data={'store': {str(new_id): value}})
            peer.enqueue_message(message)
        self.store[new_id] = value

    def peer_cleanup(self, peer):
        self._debug("Entering Congress peer cleanup.")
        for (i, bucket) in enumerate(self.buckets):
            for p in bucket:
                if p == peer or \
                p.id == peer.id or \
                p.server_address == peer.server_address or \
                p.address == peer.address:
                    self._debug("Removing peer from bucket %d" % i)
                    self.buckets[i].remove(p)
        for (i, rbucket) in enumerate(self.replacement_buckets):
            for p in rbucket:
                if p == peer or \
                p.id == peer.id or \
                p.server_address == peer.server_address or \
                p.address == peer.address:
                    self._debug("Removing peer from rbucket %d" % i)
                    self.replacement_buckets[i].remove(p)
        del(peer)

    def bootstrap_peer(self, conn_address, id=None):
        try:
            speer = self.add_peer(conn_address, connect=True)
        except socket.error:
            return False
        if id is not None:
            speer.id = id
        return speer

    def _setup_ctl_socket(self, port=29800):
        self._ctl = Controller(self, port=port)

    def __init__(self, host='0.0.0.0', port=16800, initial_peers=[],
        debug=False, ctl_port=None, pyev_loop=None):
        Clumsy.__init__(self, (host, port), client_class=CongressPeer,
            debug=debug, pyev_loop=pyev_loop)
        self._gen_id()
        self._make_buckets()
        self.store = {}
        self.register_message_handler(PING, ping_handler)
        self.register_message_handler(PONG, pong_handler)

        self.register_message_handler(RPC_STORE, handle_rpc_store)
        self.register_message_handler(RPC_GET, handle_rpc_get)
        self.register_message_handler(RPC_GET_REPLY,
            handle_rpc_get_reply)
        self.register_message_handler(RPC_FIND_NODE, handle_rpc_find_node)
        self.register_message_handler(RPC_FIND_NODE_REPLY,
            handle_rpc_find_node_reply)
        #self.register_message_handler(ID_NOTIFY, id_handler)
        self.retrieval_callbacks = []

        # Once we bootstrap a peer, ask them for all peers closest to
        # our own id.
        def node_getter(i_peer, i_server):
            i_server._hit_peer(i_peer)
            new_m = Message(RPC_FIND_NODE, data={'node_id': i_server.id})
            i_peer.enqueue_message(new_m)

        self.register_handshake(node_getter)

        if ctl_port is not None:
            self._setup_ctl_socket(ctl_port)

        for conn in initial_peers:
            self.bootstrap_peer(conn)

class CtlClient:

    def send(self, message):
        self.outgoing.append(message)

    def _ctl_ev(self, watcher, events):
        if events & pyev.EV_READ:
            # do stuff
            try:
                buffer = self.socket.recv(1024)
                if buffer != '':
                    self.curr_buff += buffer
                    if '\n' in self.curr_buff:
                        x = self.curr_buff.split('\n')
                        message = x[0] + '\n'
                        self.curr_buff = '\n'.join(x[1:])
                        self.parent.handle_message(message, self)
                else:
                    print 'A connection was closed.'
                    watcher.stop()
                    self.socket.close()
            except EOFError:
                watcher.stop()
        elif events & pyev.EV_WRITE:
            if len(self.outgoing) > 0:
                message = self.outgoing.pop(0)
                self.socket.send(message)

    def __init__(self, socket, address, controller):
        self.socket = socket
        self.address = address
        self.loop = controller.server._loop
        self._ctlwatcher = pyev.Io(self.socket, pyev.EV_READ | pyev.EV_WRITE,
           self.loop, self._ctl_ev)
        self.curr_buff = ''
        self.parent = controller
        self.outgoing = []
        self._ctlwatcher.start()

class Controller:

    def handle_message(self, message, client):
        args = message.split()
        if args[0] == 'store':
            self.server._debug('Storing a value.')
            key = args[1]
            val = args[2:]
            self.server.rpc_store(key, val)
            client.send('Value stored.\n')
        if args[0] == 'get':
            self.server._debug('Getting a value.')
            key = args[1]
            def senditback(hash_key, val):
                client.send('GET %s: %s (%s)\n' % (key, val, hash_key))
            self.server.rpc_get(key, senditback)
            self.server._debug('Finished setting up callback.')
        if args[0] == 'peer':
            conn = (args[1], int(args[2]))
            print conn
            self.server.bootstrap_peer(conn)
            client.send('Attempted to peer with %s\n' % conn)
        if args[0] == 'listpeers':
            for peer in self.server.peers:
                client.send('%s: S: %s A: %s\n' % (str(peer.id),
                    repr(peer.server_address), repr(peer.address)))
        if args[0] == 'listqueues':
            for peer in self.server.peers:
                client.send('Peer Outgoing %s: %s\n' % (str(peer.id), peer.outgoing))
                client.send('Peer Current Buffer: %s\n' % peer.curr_buff)

    def _ctl_ev(self, watcher, events):
        if events & pyev.EV_READ:
            conn, addr = self._socket.accept()
            self.clients.append(CtlClient(conn, addr, self))

    def __init__(self, server, port=29800):
        self.clients = []
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(('127.0.0.1', port))
        self._socket.listen(1)
        self._ctlwatcher = pyev.Io(self._socket,
            pyev.EV_READ | pyev.EV_WRITE, server._loop, self._ctl_ev)
        self._ctlwatcher.start()
        self.server = server

def main():
    parser = argparse.ArgumentParser(description='Run a Congress instance.')
    parser.add_argument('-H', '--host', help='Local hostname',
        default='0.0.0.0')
    parser.add_argument('-P', '--port', help='Local port',
        default=16800, type=int)
    parser.add_argument('-p', '--peer', help='Add a peer in the form of '
        'HOST PORT', nargs=2, action='append')
    parser.add_argument('-d', '--debug', help='Turn on debug output.',
        action='store_true', default=False)
    parser.add_argument('-c', '--ctl', help='Enable control socket on'
        'port 29800', default=None, type=int)
    args = parser.parse_args()

    if args.peer is not None:
        peer_conns = [(p[0], int(p[1])) for p in args.peer]
        if args.debug:
            print 'Peer connections to make: %s' % repr(peer_conns)
    else:
        peer_conns = []

    server = Congress(host=args.host, port=args.port, initial_peers=peer_conns,
        debug=args.debug, ctl_port=args.ctl)
    server.start()

if __name__ == "__main__":
    main()
