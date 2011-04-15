import pyev
import signal
import socket
import dogfood
import hashlib
import random
import traceback
from datetime import timedelta, datetime
import sys

ID_REQUEST = -1
ID_NOTIFY = -2
ACKNOWLEDGE = -3

class PeerList(list):
    
    def __del__(self, obj):
        print '*** Deleting %s with id of %d ***' % (str(obj), obj.id)
        list.__del__(self, obj)


def addr_eq(tup1, tup2):
    if tup1 is None or tup2 is None:
        return False
    tup1 = (tup1[0].encode('ascii'), int(tup1[1]))
    tup2 = (tup2[0].encode('ascii'), int(tup1[1]))
    tup1_info = socket.getaddrinfo(*tup1)
    tup2_info = socket.getaddrinfo(*tup2)
    tup1_addrs = [z[4] for z in tup1_info]
    tup2_addrs = [z[4] for z in tup2_info]
    for address in tup1_addrs:
        if address in tup2_addrs:
            return True
    return False

def dump_message(message):
    return dogfood.encode(message) + '<k!>'

def load_message(message):
    x = dogfood.decode(message[:-4])
    return x

def id_handler(message, server, peer):
    if message.data['id'] is None:
        self._debug("WARNING! ID IS NONE")
        server._remove_peer(peer)
        return
    # Check for existing connections and remove duplicates.
    
    new_id = long(message.data['id'])

    for xpeer in list(server.peers):
        # check the ID first, because it's the cheapest.
        if xpeer.id == new_id and xpeer != peer or \
            (addr_eq(xpeer.address, server._address)):
            server._debug("Peer is already present. Removing.")
            server._debug("xpeer: %s, id %d\tpeer: %s, id %d" % \
                (xpeer, xpeer.id, peer, new_id))
            server._remove_peer(peer)
            return
    
    server.peers[server.peers.index(peer)].id = new_id 
    peer.server_address = list(peer.address)
    peer.server_address[1] = message.data['server_port']
    peer.server_address = tuple(peer.server_address)
    if message.re is None:
        reply = Message(ID_NOTIFY, data={'id': server.id,
            'server_port': server._address[1]}, re=message.id)
        peer.enqueue_message(reply)
    else:
        reply = Message(ACKNOWLEDGE, re=message.id)
        peer.enqueue_message(reply)

    if not peer.active:
        peer.active = True
        for hs in server.handshakes:
            try:
                hs(peer, server)
            except Exception, e:
                traceback.print_exc(file=sys.stderr)
                raise e

        if peer.id in server.peer_handshakes:
            try:
                server.peer_handshakes[peer.id](peer, server)
                del(server.peer_handshakes[peer.id])
            except Exception, e:
                traceback.print_exc(file=sys.stderr)
                raise e

def id_requested(message, server, peer):
    m = Message(ID_NOTIFY, data={'id': server.id,
        'server_port': server._address[1]}, re=message.id)
    peer.enqueue_message(m)


class TimeoutCallback:

    def check(self):
        if datetime.now() - self.created > timedelta(seconds=self.timeout):
            # Timed out.
            self.func(*self.args, **self.kwargs)
            return True
        return False

    def __init__(self, message_id, func, timeout, args=[], kwargs={}):
        self.message_id = message_id
        self.func = func
        self.timeout = timeout
        self.created = datetime.now()
        self.args = args
        self.kwargs = kwargs

class Message(dogfood.Food):

    def __encode__(self):
        return ['Message', [self.type], {'data': self.data,
            'id': self.id, 're': self.re}]

    source = None
    dest = None

    def _gen_id(self):
        self.id = random.getrandbits(160)

    def __init__(self, message_type, data={}, id=None, re=None):
        self.type = message_type
        self.data = data
        if id is None:
            self._gen_id()
        else:
            self.id = id
        self.re = re


class Peer:

    def enqueue_message(self, message):
        message.dest = self.address
        self.outgoing.append(message)

    def __repr__(self):
        return "<CongressPeer %s %s %s>" % \
            (str(self.id), str(self.server_address), str(self.address))

    def __del__(self):
        print "__del__ occurred"
        self.stop()

    def stop(self):
        self.active = False
        #self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()
        self._sockwatcher.stop()

    def _sock_ev(self, watcher, events):
        if events & pyev.EV_READ:
            # do stuff
            try:
                buffer = self._socket.recv(1024)
                if len(buffer) > 0:
                    self.curr_buff += buffer
                    #self._parent._debug('<<< ' + buffer)
                    while '<k!>' in self.curr_buff:
                        x = self.curr_buff.split('<k!>')
                        message = load_message(x[0] + '<k!>')
                        self._parent._debug('RECV %d < %s %s' % \
                            (message.type, str(self.server_address),
                            str(self.address)))
                        self._parent._debug(str(message.data))
                        self.curr_buff = '<k!>'.join(x[1:])
                        self._parent.handle_message(message, self)
                else:
                    self._parent._debug("bufferlen is 0, removing peer")
                    self.stop()
                    self._parent._remove_peer(self)
            except EOFError:
                self._parent._debug("Received EOFError, removing peer.")
                self.stop()
                self._parent._remove_peer(self)
            except socket.error:
                #traceback.print_exc(file=sys.stderr)
                self._parent._debug("Removing peer due to socket error.")
                self._parent._remove_peer(self)
            except Exception, e:
                traceback.print_exc(file=sys.stderr)
                self._parent._debug("Giving up, removing peer.")
                self._parent._remove_peer(self)
                raise
        elif events & pyev.EV_WRITE:
            while len(self.outgoing) > 0:
                message = self.outgoing.pop(0)
                self._parent._debug('SEND %d > %s %s' % \
                    (message.type, str(self.server_address), str(self.address)))
                self._parent._debug(str(message.data))
                sbytes = dump_message(message)
                #self._parent._debug('>>> ' + sbytes)
                self._socket.sendall(sbytes)

    def __init__(self, socket, address, parent):
        self.outgoing = []
        self.curr_buff = ''
        self.active = False
        self._socket = socket
        self.address = address
        self._sockwatcher = pyev.Io(self._socket, pyev.EV_READ | pyev.EV_WRITE,
            parent._loop, self._sock_ev)
        self._parent = parent
        # We don't know this yet -- Once the client gives it to us, we
        # can reference it with communications with clients.
        self.server_address = None
        self.id = None
        self._sockwatcher.start()

class Node:

    def _debug(self, message):
        if self.debug:
            if self.debug_file is None:
                print '%s> %s' % (repr(self._address), message)
            else:
                self.debug_file.write(message + '\n')

    def _remove_peer(self, peer):
        self._debug("Adding peer to the chopping block.")
        peer.active = False
        self.prune_peers.append(peer)

    def peer_cleanup(self, peer):
        """Stub for subclasses."""
        pass

    def _timer_cb(self, watcher, events):
        if len(self.prune_peers) > 0:
            for peer in list(self.prune_peers):
                self.debug_peers()
                self._debug("Axing peer %s" % repr(peer))
                peer.stop()
                if peer in self.peers:
                    self.peers.remove(peer)
                    self._debug("Removed peer from main peer list.")
                if peer in self.prune_peers:
                    self.prune_peers.remove(peer)
                self.peer_cleanup(peer)
                self._debug("Done with peer cleanup.")
                self.debug_peers()
        for tcb in list(self.timeout_callbacks):
            result = tcb.check()
            if result:
                self.timeout_callbacks.remove(tcb)
        watcher.data += 1

    def debug_peers(self):
        self._debug('Peer list:')
        for peer in self.peers:
            self._debug('%s %s %s' % (str(peer.id), str(peer.address), str(peer.server_address)))

    def shutdown(self):
        for peer in self.peers:
            peer.stop()
            if peer in self.peers:
                self.peers.remove(peer)
            if peer in self.prune_peers:
                self.prune_peers.remove(peer)
            self.peer_cleanup(peer)
        # optional - stop all watchers
        if self._sockwatcher.data:
            print("stopping watchers: {0}".format(self._sockwatcher.data))
            for w in self._sockwatcher.data:
                w.stop()
        # unloop all nested loop
        print("stopping the loop: {0}".format(self._sockwatcher.loop))
        self._sockwatcher.loop.stop()
       

    def _sig_cb(self, watcher, events):
        print("We get signal. Quitting.")
        self.shutdown()

    def _sock_ev(self, watcher, events):
        try:
            if events & pyev.EV_READ:
                conn, addr = self._socket.accept()
                peer_added = False
                addr_info = socket.getaddrinfo(addr[0], addr[1])
                connecting_addresses = [z[4] for z in addr_info]
                for peer in self.peers:
                    if peer.address is None:
                        continue

                    # Check our IP.
                    our_info = socket.getaddrinfo(self._address[0],
                        int(self._address[1]))
                    our_addrs = [z[4] for z in our_info]
                    for our_addr in our_addrs:
                        if our_addr in connecting_addresses:
                            peer_added = True
                            break

                    # Check existing peer IPs.
                    peer_addresses = [z[4] for z in \
                        socket.getaddrinfo(peer.address[0],
                        int(peer.address[1]))]
                    for paddr in peer_addresses:
                        if paddr in connecting_addresses:
                            peer_added = True
                            break
                    if peer_added:
                        break
                if not peer_added:    
                    self.add_peer(addr, existing_socket=conn)
                else:
                    self._debug('WARNING: Peer already found.')
                    conn.close()
        except socket.error:
            traceback.print_exc(file=sys.stderr)
            self._debug("Received socket eerror. Quitting.")
            self.shutdown()
        except Exception, e:
            traceback.print_exc(file=sys.stderr)

    def register_timed(self, interval, func):
        self.timed.append((interval, func))

    def register_message_timeout(self, message, func, peer, timeout=3):
        tm = TimeoutCallback(long(message.id), func, timeout,
            args=(message, self, peer))
        self.timeout_callbacks.append(tm)

    def register_message_callback(self, message, func):
        if long(message.id) not in self.re_callbacks:
            self.re_callbacks[long(message.id)] = func

    def register_handshake(self, func, peer=None):
        """Any time a new incoming connectin is made, the argument will be
        executed with the arguments peer and the server itself."""
        if func not in self.handshakes and peer is None:
            self.handshakes.append(func)
        else:
            self.peer_handshakes[peer] = func

    def register_message_handler(self, atype, func):
        if not self.handlers.has_key(atype):
            self.handlers[atype] = []
        self.handlers[atype].append(func)

    def handle_message(self, message, peer):
        # Check if the peer is on the chopping block.
        # If it is, prune it now and stop this madness.
        if peer in self.prune_peers:
            self.prune_peers.remove(peer)
            self.peer_cleanup(peer)
            self._debug("Removed peer during handle_message.")
            return
        if message.type in self.handlers.keys():
            for handler in self.handlers[message.type]:
                try:
                    handler(message, self, peer)
                except Exception, e:
                    traceback.print_exc(file=sys.stderr)
                    raise e

        if message.re is not None:
            # Check for timeout callbacks that need to be remove as this is
            # a reply.
            tcbs = filter(lambda tcb: tcb.message_id == long(message.re),
                self.timeout_callbacks)

            for tcb in tcbs:
                self.timeout_callbacks.remove(tcb)
        
        if message.re is not None and \
            long(message.re) in self.re_callbacks.keys():
            # Check for callbacks for this reply.
            cb = self.re_callbacks[long(message.re)]
            try:
                cb(message, self, peer)
            except Exception, e:
                traceback.print_exc(file=sys.stderr)
                raise e
            del(self.re_callbacks[long(message.re)])

    def enqueue_message(self, message):
        message.source = self._address
        for peer in self.peers:
            if peer.address == message.dest:
                peer.enqueue_message(message)

    def start(self):
        self._loop.start()

    def add_peer(self, conn, connect=False, existing_socket=None):
        if existing_socket is None:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            s = existing_socket
        xpeer = self.client_class(s, conn, self)
        if connect:
            xpeer.server_address = conn 
            s.connect(conn)
        unsolicited_id = Message(ID_NOTIFY,
            data={'id': self.id, 'server_port': self._address[1]})

        # Handshake: Send our ID and server port, set active if they reply.

        def id_callback(i_message, i_server, i_peer):
            # Future use
            pass

        def id_timeout(i_message, i_server, i_peer):
                i_server._debug("Removing peer due to timeout.")
                i_server._remove_peer(i_peer)

        self.register_message_callback(unsolicited_id, id_callback)
        self.register_message_timeout(unsolicited_id, id_timeout, xpeer)
        xpeer.enqueue_message(unsolicited_id)
        self.peers.append(xpeer)
        return xpeer

    def _gen_id(self):
        self.id = random.getrandbits(160)

    def __init__(self, (host, port), client_class=Peer, debug=False,
        id=None, pyev_loop=None, debug_file=None):
        
        self.peers = PeerList()
        self.handshakes = []
        self.handlers = {}
        
        if id is None:
            self._gen_id()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((host, port))
        self._address = (host, port)
        self._socket.listen(1)
        if pyev_loop is None:
            self._loop = pyev.default_loop(io_interval=0.01)
        else:
            self._loop = pyev_loop

        self._sockwatcher = pyev.Io(self._socket, pyev.EV_READ | pyev.EV_WRITE,
            self._loop, self._sock_ev)
        self._sockwatcher.start()

        self._sigk = pyev.Signal(signal.SIGTERM, self._loop, self._sig_cb)
        self._sigk.data = [self._sockwatcher, self._sigk]
        self._sigk.start()
        self._sig = pyev.Signal(signal.SIGINT, self._loop, self._sig_cb)
        self._sig.data = [self._sockwatcher, self._sig]
        self._sig.start()

        self._timer = pyev.Timer(0, 1, self._loop, self._timer_cb, 0)
        self._timer.start()

        # Special message handling for propery message routing.


        self.register_message_handler(ID_REQUEST, id_requested)
        self.register_message_handler(ID_NOTIFY, id_handler)

        self.client_class = client_class
        self.debug = debug
        self.re_callbacks = {}
        self.timed = []
        self.prune_peers = []
        self.timeout_callbacks = []
        self.peer_handshakes = {}
 
        if debug_file is not None:
            self.debug_file = open(debug_file, 'aw')
        else:
            self.debug_file = None
