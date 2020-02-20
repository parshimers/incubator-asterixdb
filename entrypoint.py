import math,sys
class SimpleHello(object):

    def nextTuple(self, args):
        return ["a","b","c"]

    class Java:
        implements = ["org.apache.asterix.external.library.py.IPythonFunction"]

# Make sure that the python code is started first.
# Then execute: java -cp py4j.jar py4j.examples.SingleThreadClientApplication
from py4j.java_gateway import JavaGateway, CallbackServerParameters, GatewayParameters
simple_hello = SimpleHello()
port = int(sys.argv[1])
gateway = JavaGateway(
    gateway_parameters=GatewayParameters(port=port,auto_convert=True),
    callback_server_parameters=CallbackServerParameters(port=port+1),
    python_server_entry_point=simple_hello)
