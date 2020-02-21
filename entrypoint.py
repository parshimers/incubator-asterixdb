import math,sys
from py4j.java_collections import SetConverter,MapConverter,ListConverter
from py4j.java_gateway import JavaGateway, CallbackServerParameters, GatewayParameters
from py4j.java_collections import SetConverter,MapConverter,ListConverter
import pickle;
class Wrapper(object):

    def __init__(self,gateway):
        self.gateway = gateway
        f = open('sentiment_pipeline3','rb')
        self.pipeline = pickle.load(f)
        f.close()

    def nextTuple(self, args):
        ret = self.nextTuple_priv(args)
        if isinstance(ret,list):
            return ListConverter().convert(ret,self.gateway["gw"]._gateway_client)
        if isinstance(ret,dict):
            return MapConverter().convert(ret,self.gateway["gw"]._gateway_client)
        return ret

    def nextTuple_priv(self,args):
        return self.pipeline.predict(args)[0].item()


    class Java:
        implements = ["org.apache.asterix.external.library.py.PythonFunction$IPythonFunction"]

# Make sure that the python code is started first.
# Then execute: java -cp py4j.jar py4j.examples.SingleThreadClientApplication
port = int(sys.argv[1])
foo = {"gw": None}
foo["gw"] = JavaGateway(
    gateway_parameters=GatewayParameters(port=port, auto_convert=True, auto_field=True),
    callback_server_parameters=CallbackServerParameters(port=port+1),
    python_server_entry_point=Wrapper(foo))
