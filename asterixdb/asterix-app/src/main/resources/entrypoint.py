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
from struct import *

PROTO_VERSION = 1
HEADER_SZ = 4+8+8+1


class MessageType(IntEnum):
    HELO = 0
    QUIT = 1
    INIT = 2
    INIT_RSP = 3
    CALL = 4
    CALL_RSP = 5

class MessageFlags(IntEnum):
    NORMAL = 0
    INITIAL_REQ = 1
    INITIAL_ACK = 2
    ERROR = 3


class Wrapper(object):
    wrapped_module = None
    wrapped_class = None
    wrapped_fn = None
    packer = msgpack.Packer(autoreset=False)
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
        self.wrapped_fns[self.rmid] = wrapped_fn
        return True

    def nextTuple(self, *args, key=None):
        fun = self.wrapped_fns[key]
        return self.wrapped_fns[key](*args)

    def check_module_path(self, module):
        cwd = Path('.').resolve()
        module_path = Path(module.__file__).resolve()
        return cwd in module_path.parents

    def read_header(self,readbuf):
        self.sz,self.mid,self.rmid,self.flag = unpack("!iqqb",readbuf[0:21])
        print(self.sz,self.mid,self.rmid,self.flag)
        #TODO: nuh
        return True

    def write_header(self,response_buf,dlen):
        total_len = dlen + HEADER_SZ-4;
        header = pack("!iqqb",total_len,int(-1),int(self.rmid),self.flag)
        self.response_buf.write(header)

    def get_ver_hlen(self, hlen):
        return hlen + (PROTO_VERSION << 4)

    def get_hlen(self):
        return self.ver_hlen - (PROTO_VERSION << 4)

    def helo(self):
        print("HELO")
        self.response_buf.seek(0)
        self.flag = int(MessageFlags.INITIAL_REQ)
        dlen = len(self.unpacked_msg[1])
        self.write_header(self.response_buf,dlen)
        self.response_buf.write(self.unpacked_msg[1])
        self.resp = self.response_buf.getvalue()
        self.send_msg()
        self.packer.reset()
        return True

    def handle_init(self):
        print("INIT")
        self.flag = 0
        self.response_buf.seek(0)
        args = self.unpacked_msg[1]
        module = args[0]
        clazz = args[1]
        fn = args[2]
        self.init(module, clazz, fn)
        self.packer.pack(int(MessageType.INIT_RSP))
        # TODO: would die if you had more than 128 functions per interpreter..
        dlen = 1
        self.write_header(self.response_buf,dlen)
        self.response_buf.write(self.packer.bytes())
        self.resp = self.response_buf.getvalue()
        self.send_msg()
        self.packer.reset()
        return True

    def quit(self):
        self.alive = False

    def handle_call(self):
        print("CALL")
        self.flag = MessageFlags.NORMAL
        result = self.nextTuple(self.unpacked_msg[1], key=self.rmid)
        self.packer.reset()
        self.response_buf.seek(0)
        body = msgpack.packb(result)
        dlen = len(body)+1
        self.write_header(self.response_buf,dlen)
        self.packer.pack(int(MessageType.CALL_RSP))
        self.response_buf.write(self.packer.bytes())
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
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.connect(("127.0.0.1", int(port)))
        except socket.error as msg:
            print(sys.stderr, msg)

    def disconnect_sock(self):
        self.sock.close()

    def recv_msg(self):
        completed = False
        header_read = False
        while not completed:
            readbuf = sys.stdin.buffer.read1(8192)
            print(len(readbuf))
            if not readbuf:
                break
            #TODO: aaaaaaaaaaaaAAA
            header_read = self.read_header(readbuf)
            print(self.flag)
            self.unpacker.feed(readbuf[21:])
            self.unpacked_msg = list(self.unpacker)
            self.type = MessageType(self.unpacked_msg[0])
            completed = self.type_handler[self.type](self)

    def send_msg(self):
        print(self.flag)
        self.sock.sendall(self.resp)
        return

    def recv_loop(self):
        while self.alive:
            self.recv_msg()


port = str(sys.argv[1])
wrap = Wrapper()
wrap.connect_sock(port)
#wrap.helo()
wrap.recv_loop()
