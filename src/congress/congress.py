import sys
import random
import hashlib
import socket
import traceback
import argparse
import pyev
from node import *
from control import *
from _congress import * 

k = 20
a = 3

PING = 1
PONG = 2
RPC_STORE = 5
RPC_FIND_NODE = 6
RPC_GET = 7
RPC_FIND_NODE_REPLY = 8
RPC_GET_REPLY = 9
RPC_CHAT = 10

def sha1_hash(key):
    """
    Generate a SHA1 hash for a given string.

    :param key:
    """
    sha1 = hashlib.sha1()
    sha1.update(key)
    new_id = long(sha1.digest().encode('hex'), 16)
    return new_id

def ping_handler(message, server, peer):
    """
    Callback for PING messages.
    Send back a PONG.

    :param message: PING message
    :param server: The Congress server
    :param peer: The peer the message was received from
    """
    reply = Message(PONG, re=message.id)
    peer.enqueue_message(reply)

def pong_handler(message, server, peer):
    """
    Callback for PONG messages.
    Refresh the k-bucket information for the given peer.

    :param messages: PONG message
    :param server: The Congress server
    :param peer: The peer the message was received from
    """
    if peer.active:
        server._hit_peer(peer)

def handle_rpc_store(message, server, peer):
    """
    Callback for STORE messages.
    Store the hashed key: value in our store.

    :param message: STORE message
    :param server: The Congress server
    :param peer: The peer the message was received from
    """
    for key in message.data['store'].keys():
        server.store[long(key)] = message.data['store'][key]

def chat_handler(message, server, peer):
    """
    Callback for CHAT messages. Congress makes no attempt to encrypt or
    secure the message from eavesdropping, and leaves any implementation of
    chat message security to the developer.

    To handle chat messages intended for the node itself, register your
    callback with register_chat_callback.

    :param message: CHAT message
    :param server: The Congress server
    :param peer: The peer hte message was received from
    """
    target_node = message.data['target_node_id']
    source_node = message.data['source_node_id']
    chat_message = message.data['chat_message']
    if target_node == server.id:
        for cb in server.chat_callbacks:
            try:
                cb(source_node, chat_message, server, peer)
            except Exception, e:
                traceback.print_exc(file=sys.stderr)
    else:
        closest = server._closest_peers(target_node, a)
        for npeer in closest:
            m = Message(RPC_CHAT, data={
                'target_node_id': target_node,
                'source_node_id': source_node,
                'chat_message': chat_message
                })
            npeer.enqueue_message(m)


def handle_rpc_get(message, server, peer):
    """
    Callback for GET messages.
    If we have the value, send it back to the peer.
    If we don't, build a list of alpha closest peers and send it back to the
    peer.

    :param message: GET message
    :param server: The Congress server
    :param peer: The peer the message was receved from
    """
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
    """
    Check if a given hashed key and value are present in the store.

    :param message: This function is only called from the GET reply, so the
        message in the callback is passed to this function.
    :param server: Same deal as the message, this is the Congress server.
    """
    server._debug('Retrieval callback length: %d' % \
        len(server.retrieval_callbacks))
    for tup in list(server.retrieval_callbacks):
        (key, callback) = tup
        hash_key = long(sha1_hash(key))
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
    """
    Callback for GET replies.
    If we get the value back in the message, run value_check.
    If we get a list of peers, bootstrap them and request the same
    value, but only if we don't have the value.

    :param message: The GET reply itself.
    :param server: The Congress server
    :param peer: The peer that sent the message
    """
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
        if long(message.data['key']) in server.store or attempt > k:
            return
        for (node_id, address, port) in message.data['peers']:
            node_id = long(node_id)
            if node_id == server.id:
                continue
            request = Message(RPC_GET,
                data={'key': message.data['key'], 'attempt': attempt + 1},
                re=message.id)
            server._debug("Attempting to bootstrap peer %s " % str(node_id) + \
                "via RPC_GET_REPLY message.")
            if node_id not in [pee.id for pee in server.peers] and \
                node_id != server.id:
                server._debug('New node.')
                def request_storage(xxpeer, server):
                    xxpeer.enqueue_message(request)
                server.register_handshake(request_storage, peer=node_id)
                xpeer = server.bootstrap_peer((address, port), id=node_id)
            else:
                server._debug('Existing node.')
                matched = filter(lambda pee: pee.id == node_id, server.peers)
                if len(matched) > 0:
                    matched[0].enqueue_message(request)


def handle_rpc_find_node(message, server, peer):
    """
    Callback for FIND_NODE messages.
    Pass back k closest peers to the ID.

    :param message: The FIND_NODE message itself.
    :param server: The Congress server
    :param peer: The peer that sent the message.
    """
    new_id = message.data['node_id']
    closest = server._closest_peers(new_id, k)
    m = Message(RPC_FIND_NODE_REPLY,
        re=message.id,
        data={'peers': [(p.id, p.server_address[0], p.server_address[1]) for \
                p in closest]})
    peer.enqueue_message(m)

def handle_rpc_find_node_reply(message, server, peer):
    """
    Callback for FIND_NODE replies.
    Bootstrap any unseen peers.

    :param message: The FIND_NODE reply.
    :param server: The Congress server
    :param peer: The peer that sent the reply.
    """
    peer_tuples = message.data['peers']
    server_ids = [p.id for p in server.peers]
    for (node_id, address, port) in \
        filter(lambda e: e[0] not in server_ids and e[0] != server.id,
        peer_tuples):
        new_peer = server.bootstrap_peer((address, port), id=node_id)


class CongressPeer(Peer):
    """Peer subclass for Congress. Currently no real logic is here."""

    def __del__(self):
        Peer.__del__(self)

    def __init__(self, conn, addr, server):
        Peer.__init__(self, conn, addr, server)

class Congress(Node):
    """Main DHT class. The main event loop is a member of this class, and all
    peers and other pyev callbacks attach to this loop."""

    def _debug(self, message):
        Node._debug(self, message)
        if self._ctl is not None and self.debug:
           self._ctl.broadcast(message)

    def _hit_peer(self, peer):
        """
        Update the node's k-buckets with the peer, using a simplified version
        of the kademlia specification's routine for updating k-buckets.

        :param peer:
        """
        if not peer.active:
            self._debug("Peer not active. What.")
            return
        if peer.id is None:
            self._debug("Peer ID None during _hit_peer.")
            return
        dist = peer.id ^ self.id
        matching_buckets = filter(lambda (i, b): bucket_leaf(dist, i),
            enumerate(self.buckets))
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
        """Determine if any of the connected peers have the specified 160-bit
        node ID.
        """
        for bucket in self.buckets:
            for peer in bucket:
                if type(peer.id) != long:
                    peer.id = long(peer.id)
                if peer.id == node_id:
                    return True
        return False

    def _conn_present(self, conn):
        """
        Determine if the given (address, port) tuple is presently connected
        via any of the peers.
        """
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
        """
        At node, start, create 160 k-buckets and 160 replacement buckets.
        Currently, replacement buckets are not used but the peer is placed into
        them if a bucket's peer count is over k.
        """
        self.buckets = []
        self.replacement_buckets = []
        for i in range(160):
            self.buckets.append([])
            self.replacement_buckets.append([])

    def _closest_peers(self, id, how_many, filter_id=None):
        """
        Given the specified 160-bit ID, use the k-buckets to return, at most,
        how_many closest peers to the ID using a XOR metric (id XOR peer.id)

        :param id: 160-bit ID
        :param how_many: The maximum number of peers to return, usually k.
        """
        self._debug("Finding %d closest peers." % how_many)
        try:
            dist = self.id ^ id
            cl_buckets = [t[1] for t in sorted(enumerate(self.buckets),
                key=lambda x: bucket_sort(dist, x[0]))]
            closest = []
            i = 0
            while len(closest) < k and i < 160:
                closest.extend(filter(lambda p: p.active and p.id != filter_id,
                    cl_buckets[i]))
                i += 1
            if len(closest) > k:
                closest = closest[:k]
            return closest
        except Exception, e:
            traceback.print_exc(file=sys.stderr)

    def rpc_get(self, key, callback):
        """Since value retrieval is async, provide a callback that will handle
        the value.

        :param key: String key
        :param callback: Callable to execute when we get the value.
        """
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
        """
        Store hashed key: val with k closest connected peers.

        :param key: string key
        :param value: Any python value that is json serializable.
        """
        new_id = sha1_hash(key)
        for peer in self._closest_peers(new_id, k):
            message = Message(RPC_STORE, data={'store': {str(new_id): value}})
            peer.enqueue_message(message)
        self.store[new_id] = value
        if hasattr(self.store, 'sync'):
            self.store.sync()

    def rpc_chat(self, node_id, chat_message):
        """
        Initiate an RPC_CHAT message.

        :param node_id: The 160-bit ID of the peer you'd like to send a 
            message to. It need not be a connected peer, just a known ID.
        :param chat_message: The message to send.
        """

        matched_peers = filter(lambda p: p.id == node_id, self.peers)
        if len(matched_peers) > 0:
            m = Message(RPC_CHAT, data={'target_node_id': node_id,
                'source_node_id': self.id, 'chat_message': chat_message})
            matched_peers[0].enqueue_message(m)
        else:
            closest = self._closest_peers(node_id, a)
            m = Message(RPC_CHAT, data={'target_node_id': node_id,
                'source_node_id': self.id, 'chat_message': chat_message})
            for cpeer in closest:
                cpeer.enqueue_message(m)

    def register_chat_callback(self, callback):
        """
        Store a callable in our chat callbacks list. Each will be called in
        order of registration, when we receive an RPC_CHAT with a
        target_node_id that matches our server's 160-bit ID.

        The signature of the callable should be as follows:
            cb(source_node, chat_message, server, peer)
        """
        self.chat_callbacks.append(callback)

    def peer_cleanup(self, peer):
        """
        When error handling or explicit peer removal schedules a peer object
        for removal, this function is called to remove it from all applicable
        buckets by the core node.py removal routine.
        """
        self._debug("Entering Congress peer cleanup.")
        for (i, bucket) in enumerate(self.buckets):
            for p in bucket:
                if p == peer or \
                p.id == peer.id:
                    self._debug("Removing peer from bucket %d" % i)
                    self.buckets[i].remove(p)
        for (i, rbucket) in enumerate(self.replacement_buckets):
            for p in rbucket:
                if p == peer or \
                p.id == peer.id:
                    self._debug("Removing peer from rbucket %d" % i)
                    self.replacement_buckets[i].remove(p)
        del(peer)

    def bootstrap_peer(self, conn_address, id=None):
        """
        Add a peer. This function does not add it to k-buckets,
        as this is handled by the final handshake callback.

        :param conn_address: (hostname, port) tuple of the peer
        :param id: The ID is supplied if FIND_NODE replies provide the peer.
        """
        try:
            speer = self.add_peer(conn_address, connect=True)
        except socket.error:
            return False
        if id is not None:
            speer.id = id
        return speer

    def _setup_ctl_socket(self, port=29800):
        """
        Setup a socket to listen for simple control connections.

        :param port: The port that local connections can use.
        """
        self._ctl = Controller(self, port=port)

    def __init__(self, host='0.0.0.0', port=16800, initial_peers=[],
        debug=False, ctl_port=None, pyev_loop=None, debug_file=None,
        storage_class=dict):
        """
        :param initial_peers: List of (hostname, port) tuples to connect to.
        :param debug: If True, many debug messages will be printed.
        :param ctl_port: Local port for the control socket.
        :param pyev_loop: If you have your own pyev loop to attach the node to,
            supply it here.
        """
        Node.__init__(self, (host, port), client_class=CongressPeer,
            debug=debug, pyev_loop=pyev_loop, debug_file=debug_file)
        self._gen_id()
        self._make_buckets()
        self.store = storage_class()
        self.register_message_handler(PING, ping_handler)
        self.register_message_handler(PONG, pong_handler)

        self.register_message_handler(RPC_STORE, handle_rpc_store)
        self.register_message_handler(RPC_GET, handle_rpc_get)
        self.register_message_handler(RPC_GET_REPLY,
            handle_rpc_get_reply)
        self.register_message_handler(RPC_FIND_NODE, handle_rpc_find_node)
        self.register_message_handler(RPC_FIND_NODE_REPLY,
            handle_rpc_find_node_reply)
        self.register_message_handler(RPC_CHAT, chat_handler)
        self.retrieval_callbacks = []
        self.chat_callbacks = []

        # Once we bootstrap a peer, ask them for all peers closest to
        # our own id.
        def node_getter(i_peer, i_server):
            """
            FIND_NODE callback for the initial handshake.
            :param i_peer: The peer the handshake is with.
            :param i_server: Our node object.
            """
            i_server._hit_peer(i_peer)
            new_m = Message(RPC_FIND_NODE, data={'node_id': i_server.id})
            i_peer.enqueue_message(new_m)

        self.register_handshake(node_getter)

        if ctl_port is not None:
            self._setup_ctl_socket(ctl_port)
        else:
            self._ctl = None


        for conn in initial_peers:
            self.bootstrap_peer(conn)


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
    parser.add_argument('-F', '--debugfile', help='Debug output is saved'
        'in the argument to this option', default=None)
    args = parser.parse_args()

    if args.peer is not None:
        peer_conns = [(p[0], int(p[1])) for p in args.peer]
        if args.debug:
            print 'Peer connections to make: %s' % repr(peer_conns)
    else:
        peer_conns = []

    server = Congress(host=args.host, port=args.port, initial_peers=peer_conns,
        debug=args.debug, ctl_port=args.ctl, debug_file=args.debugfile)
    server.start()

if __name__ == "__main__":
    main()
