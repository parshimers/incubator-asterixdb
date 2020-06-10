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

import math,sys
sys.path.insert(0,'./site-packages/')
import msgpack
import socket
import os
import sys
import time
from importlib import import_module
from inspect import signature
from pathlib import Path
from enum import IntEnum

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

    wrapped_fns = {}


    def init(self, module_name, class_name, fn_name):
        self.wrapped_module = import_module(module_name)
        # do not allow modules to be called that are not part of the uploaded module
        if not self.check_module_path(self.wrapped_module):
            wrapped_module = None
            return None
        if class_name is not None:
            self.wrapped_class = getattr(import_module(module_name),class_name)()
        if self.wrapped_class is not None:
            wrapped_fn = getattr(self.wrapped_class,fn_name)
            self.wrapped_fns[(module_name,class_name,fn_name)] = wrapped_fn
        else:
            wrapped_fn = locals()[fn_name]
            self.wrapped_fns[(module_name,class_name,fn_name)] = wrapped_fn


    def nextTuple(self, *args, key=None):
        fun = self.wrapped_fns[key]
        print(args)
        print(fun)
        return self.wrapped_fns[key](*args)

    def check_module_path(self,module):
        cwd = Path('.').resolve()
        module_path = Path(module.__file__).resolve()
        return cwd in module_path.parents

    def read_header(self):
        #dgaf about this for rn
        header = self.unpacked_msg
        self.ver_hlen = header[0]
        self.type = MessageType(header[1])
        self.dlen = header[2]
        return True


    def helo(self):
        resp_body = msgpack.packb("helo")
        dlen = len(resp_body)
        typ = MessageType.HELO
        resp = bytearray(dlen+7)
        self.packer.pack(23)
        self.packer.pack(int(typ))
        self.packer.pack(dlen)
        self.packer.pack("helo")
        self.resp = self.packer.getbuffer()
        self.send_msg()
        self.packer.reset()

    def handle_init(self):
        #TODO: isn't right. needs to handle module,fn too
        args = self.unpacked_msg[3]
        module = args[0]
        clazz = args[1]
        fn = args[2]
        self.init(module,clazz,fn)
        self.packer.pack(23)
        self.packer.pack(int(MessageType.INIT_RSP))
        self.packer.pack(3)
        self.packer.pack("ok")
        self.resp = self.packer.getbuffer()
        self.send_msg()
        self.packer.reset()
        return True

    def quit(self):
        return

    def handle_call(self):
        args = self.unpacked_msg[3]
        module = args[0]
        clazz = args[1]
        fn = args[2]
        key = (module,clazz,fn)
        print(self.unpacked_msg[4])
        print(key)
        result = self.nextTuple(self.unpacked_msg[4],key=key)
        print(result)
        #TODO: NO NO NO NO
        resultsz = len(msgpack.packb(result))
        self.packer.pack(23)
        self.packer.pack(int(MessageType.CALL_RSP))
        self.packer.pack(resultsz)
        print(result)
        self.packer.pack(result)
        self.resp = self.packer.getbuffer()
        self.send_msg()
        self.packer.reset()
        return True

    type_handler = {
        MessageType.HELO: helo,
        MessageType.QUIT: quit,
        MessageType.INIT: handle_init,
        MessageType.CALL: handle_call
    }

    def connect_sock(self,sock_name):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            self.sock.connect(sock_name)
        except socket.error as msg:
            print(sys.stderr, msg)

    def disconnect_sock(self):
        self.sock.close()

    def recv_msg(self):
        #TODO: make this work with bigger than 4k records
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
            #TODO: hlen is not static
            if len(readbuf) < self.dlen-5:
                readbuf += self.sock.recv(self.dlen-len(readbuf))
                self.unpacker.feed(readbuf)

            completed = self.type_handler[self.type](self)

    def send_msg(self):
        self.sock.sendall(self.resp)
        return

    def recv_loop(self):
        while True:
            self.recv_msg()

sock_name = str(sys.argv[1])
wrap = Wrapper()
time.sleep(2)
wrap.connect_sock(sock_name)
wrap.helo()
wrap.recv_loop()
