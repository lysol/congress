import pyev

class CtlClient:
    """Clients connected to the control port are interacted with via this
    class."""

    def send(self, message):
        """
        Send bytes back to the client.

        :param message: A normal, mundane string. You must append newlines
            manually.
        """
        self.outgoing.append(message)

    def _ctl_ev(self, watcher, events):
        """
        Main pyev callback for the controller.
        """
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
        """
        Usage:

        >>> client = CtlClient(socket, address, controller)

        :param socket: socket
        :type socket: socket.socket
        :return: `Instance of CtlClient`
        :rtype: CtlCient
        """
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
        """
        When we receive a message, parse commands from it.
        """
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
        """
        This is the pyev callback for the controller to receive new
        connections.
        """
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
