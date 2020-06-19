# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys
sys.path.insert(0, './site-packages/')
from io import BytesIO
from enum import IntEnum
from pathlib import Path
from importlib import import_module
import socket
import msgpack

PROTO_VERSION = 1


class MessageType(IntEnum):
    HELO = 0
    QUIT = 1
    INIT = 2
    INIT_RSP = 3
    CALL = 4
    CALL_RSP = 5


class Wrapper(object):
    wrapped_module = None
    wrapped_class = None
    wrapped_fn = None
    packer = msgpack.Packer(use_bin_type=False, autoreset=False)
    unpacker = msgpack.Unpacker()
    response_buf = BytesIO()
    wrapped_fns = {}
    fn_id_next = 0
    alive = True

    def init(self, module_name, class_name, fn_name):
        self.wrapped_module = import_module(module_name)
        # do not allow modules to be called that are not part of the uploaded module
        wrapped_fn = None
        if not self.check_module_path(self.wrapped_module):
            wrapped_module = None
            return None
        if class_name is not None:
            self.wrapped_class = getattr(
                import_module(module_name), class_name)()
        if self.wrapped_class is not None:
            wrapped_fn = getattr(self.wrapped_class, fn_name)
        else:
            wrapped_fn = locals()[fn_name]
        fn_id = self.fn_id_next
        self.wrapped_fns[fn_id] = wrapped_fn
        self.fn_id_next = self.fn_id_next + 1
        return fn_id

    def nextTuple(self, *args, key=None):
        fun = self.wrapped_fns[key]
        return self.wrapped_fns[key](*args)

    def check_module_path(self, module):
        cwd = Path('.').resolve()
        module_path = Path(module.__file__).resolve()
        return cwd in module_path.parents

    def read_header(self):
        header = self.unpacked_msg
        self.ver_hlen = header[0]
        self.type = MessageType(header[1])
        self.dlen = header[2]
        return True

    def get_ver_hlen(self, hlen):
        return hlen + (PROTO_VERSION << 4)

    def get_hlen(self):
        return self.ver_hlen - (PROTO_VERSION << 4)

    def helo(self):
        typ = MessageType.HELO
        self.packer.pack(self.get_ver_hlen(3))
        self.packer.pack(int(typ))
        self.packer.pack(5)
        self.packer.pack("helo")
        self.resp = self.packer.getbuffer()
        self.send_msg()
        self.packer.reset()

    def handle_init(self):
        args = self.unpacked_msg[3]
        module = args[0]
        clazz = args[1]
        fn = args[2]
        fn_id = self.init(module, clazz, fn)
        self.packer.pack(self.get_ver_hlen(3))
        self.packer.pack(int(MessageType.INIT_RSP))
        # TODO: would die if you had more than 128 functions per interpreter..
        self.packer.pack(1)
        self.packer.pack(int(fn_id))
        self.resp = self.packer.getbuffer()
        self.send_msg()
        self.packer.reset()
        return True

    def quit(self):
        self.alive = False

    def handle_call(self):
        key = self.unpacked_msg[3]
        result = self.nextTuple(self.unpacked_msg[4], key=key)
        self.packer.reset()
        self.response_buf.seek(0)
        body = msgpack.packb(result)
        dlen = msgpack.packb(len(body))
        hlen = len(dlen) + 2  # ver_hlen + type
        ver_hlen = self.get_ver_hlen(hlen)
        self.packer.pack(int(ver_hlen))
        self.packer.pack(int(MessageType.CALL_RSP))
        self.response_buf.write(self.packer.bytes())
        self.response_buf.write(dlen)
        self.response_buf.write(body)
        self.resp = self.response_buf.getvalue()
        self.send_msg()
        self.packer.reset()
        return True

    type_handler = {
        MessageType.HELO: helo,
        MessageType.QUIT: quit,
        MessageType.INIT: handle_init,
        MessageType.CALL: handle_call
    }

    def connect_sock(self, sock_name):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            self.sock.connect(sock_name)
        except socket.error as msg:
            print(sys.stderr, msg)

    def disconnect_sock(self):
        self.sock.close()

    def recv_msg(self):
        completed = False
        header_read = False
        while not completed:
            readbuf = self.sock.recv(4096)
            if not readbuf:
                break
            self.unpacker.feed(readbuf)
            self.unpacked_msg = list(self.unpacker)
            if not header_read:
                self.read_header()
                header_read = True
            if len(readbuf) < self.dlen+self.get_hlen():
                readbuf = readbuf + self.sock.recv(self.dlen-len(readbuf))
                self.unpacker.feed(readbuf)

            completed = self.type_handler[self.type](self)

    def send_msg(self):
        self.sock.sendall(self.resp)
        return

    def recv_loop(self):
        while self.alive:
            self.recv_msg()


sock_name = str(sys.argv[1])
wrap = Wrapper()
wrap.connect_sock(sock_name)
wrap.helo()
wrap.recv_loop()
