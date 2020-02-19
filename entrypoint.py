import math,sys
class SimpleHello(object):

    def sayHello(self, int_value=None, string_value=None):
        print(int_value, string_value)
        return "Said hello to {0}".format(string_value)

    def sqrt(self, arg):
        return math.sqrt(arg)

    class Java:
        implements = ["py4j.examples.IHello"]

# Make sure that the python code is started first.
# Then execute: java -cp py4j.jar py4j.examples.SingleThreadClientApplication
from py4j.java_gateway import JavaGateway, CallbackServerParameters, GatewayParameters
simple_hello = SimpleHello()
port = int(sys.argv[1])
gateway = JavaGateway(
    gateway_parameters=GatewayParameters(port=port),
    callback_server_parameters=CallbackServerParameters(port=port+1),
    python_server_entry_point=simple_hello)
python_port = gateway.get_callback_server().get_listening_port()
