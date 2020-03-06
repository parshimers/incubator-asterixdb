import math,sys
import Pyro4
from pydoc import locate

@Pyro4.expose
class Wrapper(object):
    wrapped_class = None
    wrapped_fn = None

    def __init__(self, class_name, fn_name):
        if class_name is not None:
            wrapped_class = locate(class_name)
        if wrapped_class is not None:
            wrapped_fn = getattr(wrapped_class,fn_name)
        else:
            wrapped_fn = locals()[fn_name]

    def nextTuple(self, *args):
        return wrapped_fn(args)

    def ping(self):
        return "pong"

# Make sure that the python code is started first.
# Then execute: java -cp py4j.jar py4j.examples.SingleThreadClientApplication
#Pyro4.config.SERVERTYPE='multiplex'
#Pyro4.config.SERIALIZER='msgpack'
#Pyro4.config.SERIALIZERS_ACCEPTED = {'msgpack'}
port = int(sys.argv[1])
wrap = Wrapper(sys.argv[2],sys.argv[3])
d = Pyro4.Daemon(host="127.0.0.1",port=port)
d.register(wrap,"nextTuple")
print(Pyro4.config.dump())
d.requestLoop()
