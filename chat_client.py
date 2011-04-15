from Tkinter import *
from Tkinter import Message as tkMessage
from congress import *
import pyev

root = Tk()


class MainWindow:

    def set_chat(self, node_id, message):
        self.chat_messages.append((node_id, message))
        self.chat_output.set('\n'.join(['%s: %s' % (str(t[0]), t[1]) for t in \
            self.chat_messages]))

    def __init__(self, root, chat_entry_callback, node_id=0):
        self.root = root
        self.node_label = Label(root, text="Node ID: %d" % node_id)
        self.label = Label(root, text='Chat:')
        self.chat_entry = Entry(root)
        self.chat_entry.bind('<Return>',
            lambda e: chat_entry_callback(self.chat_entry))
        #self.chat_scroll = Scrollbar(root)
        self.chat_messages = []
        self.chat_output = StringVar()
        self.chat_display = tkMessage(root)
        self.chat_display.config(textvariable=self.chat_output)
        #self.chat_display.config(yscrollcommand=self.chat_scroll.set)
        #self.chat_scroll.config(command=self.chat_display.yview)

    def draw(self):
        self.node_label.grid(row=0, column=0, columnspan=2)
        self.label.grid(row=1, column=0)
        self.chat_entry.grid(row=1, column=1)
        self.chat_display.grid(columnspan=2, row=2, column=0)
        #self.chat_scroll.pack(fill=y, side=RIGHT)


class ChatClient:


    def start(self):
        self.node.start()

    def print_text(self, widget):
        text = widget.get()
        widget.delete(0, END)
        print text

    def _idle_ev(self, watcher, events):
        try:
            self.main_window.draw()
            self.root.update()
        except TclError:
            self.node.shutdown()

    def handle_chat(self, source_node, chat_message, server, peer):
        print 'test'
        self.main_window.set_chat(source_node, chat_message)

    def __init__(self, root):
        self.root = root    
        self.node = Congress(debug=True)
        self.node.register_chat_callback(self.handle_chat)
        self.idle_watcher = pyev.Timer(0.0, 0.00001, self.node._loop, self._idle_ev)
        self.idle_watcher.start()
        self.main_window = MainWindow(root,
            chat_entry_callback=self.print_text, node_id=self.node.id)
        print 'Our ID: %d' % self.node.id
        self.root.bind('<Escape>', lambda e: self.node.shutdown())
        self.chat_output = StringVar()
        self.counter = 0

client = ChatClient(root)
client.start()
