import math,sys
sys.path.insert(0,'./site-packages/')
import Pyro4
from importlib import import_module
from pathlib import Path

@Pyro4.expose
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
        return self.wrapped_fn(args)

    def ping(self):
        return "pong"

    def check_module_path(self,module):
        cwd = Path('.').resolve()
        module_path = Path(module.__file__).resolve()
        return cwd in module_path.parents


port = int(sys.argv[1])
wrap = Wrapper(sys.argv[2],sys.argv[3],sys.argv[4])
d = Pyro4.Daemon(host="127.0.0.1",port=port)
d.register(wrap,"nextTuple")
print(Pyro4.config.dump())
d.requestLoop()
