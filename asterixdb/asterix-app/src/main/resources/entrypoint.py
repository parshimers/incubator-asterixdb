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
from importlib import import_module
from pathlib import Path
from enum import Enum

class MessageType(Enum):
    HELO = 1
    QUIT = 2
    INIT = 3
    INIT_RSP = 4
    CALL = 5
    CALL_RSP = 6

class Wrapper(object):
    wrapped_module = None
    wrapped_class = None
    wrapped_fn = None


    def __init__(self, module_name, class_name, fn_name):
        self.wrapped_module = import_module(module_name)
        # do not allow modules to be called that are not part of the uploaded module
        if not self.check_module_path(self.wrapped_module):
            wrapped_module = None
            return None
        if class_name is not None:
            self.wrapped_class = getattr(import_module(module_name),class_name)()
        if self.wrapped_class is not None:
            self.wrapped_fn = getattr(self.wrapped_class,fn_name)
        else:
            self.wrapped_fn = locals()[fn_name]

    def nextTuple(self, *args):
        position = args[1]
        return msgpack.packb(self.wrapped_fn(msgpack.unpackb(args[0][:position])))

    def ping(self):
        return msgpack.packb("pong")

    def check_module_path(self,module):
        cwd = Path('.').resolve()
        module_path = Path(module.__file__).resolve()
        return cwd in module_path.parents

    def read_header(self,bytes):
        #dgaf about this for rn
        self.ver_hlen = bytes[0]
        self.type = bytes[1]
        self.dlen = int(bytes[2:6])
        self.body = bytes[7:self.dlen]


    def helo(self):
        resp = msgpack.packb("helo")
        dlen = len(resp)
        type = MessageType.HELO
        resp = bytes[dlen+7]
        resp[0] = self.ver_hlen
        resp[1] = bytes(type)
        resp[2:6] = dlen
        resp[7:dlen] = resp
        return

    def init(self):
        return
    def quit(self):
        return
    def call(self):
        return

    type_handler = {
        MessageType.HELO: helo(),
        MessageType.QUIT: quit(),
        MessageType.INIT: init(),
        MessageType.CALL: call()
    }

    def read_body(self):
        unpacked_body = msgpack.unpackb(self.body)
        self.type_handler[self.type]()

    def connect_sock(self,sock_name):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            self.sock.connect(sock_name)
        except socket.error as msg:
            print(sys.stderr, msg)

    def disconnect_sock(self):
        self.sock.close()

    def recv_msg(self):
        return

    def send_msg(self):
        return




sock_name = str(sys.argv[1])
config.SERIALIZER = "msgpack"
config.SOCK_NODELAY= "True"
wrap = Wrapper(sys.argv[2],sys.argv[3],sys.argv[4])
wrap.connect_sock(sock_name)
wrap.helo()
