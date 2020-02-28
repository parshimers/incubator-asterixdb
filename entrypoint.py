import math,sys
import Pyro4
import pickle;

@Pyro4.expose
class Wrapper(object):

    def __init__(self):
        f = open('sentiment_pipeline3','rb')
        self.pipeline = pickle.load(f)
        f.close()

    def sentiment(self, *args):
        return self.pipeline.predict(args)[0].item()

    def ping(self):
        return "pong"

# Make sure that the python code is started first.
# Then execute: java -cp py4j.jar py4j.examples.SingleThreadClientApplication
Pyro4.config.SERVERTYPE='multiplex'
Pyro4.config.SERIALIZER='msgpack'
Pyro4.config.SERIALIZERS_ACCEPTED = {'msgpack'}
port = int(sys.argv[1])
sent = Wrapper()
d = Pyro4.Daemon(host="127.0.0.1",port=port)
d.register(sent,"sentiment")
print(Pyro4.config.dump())
d.requestLoop()
