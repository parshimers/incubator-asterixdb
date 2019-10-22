import json

def asterixInitialize():
    return None


def asterixDeinitialize():
    return None


def nextFrame(asterixArgs):
    args = {}
    args["given"] = asterixArgs[0]
    args["hello"] = "world"
    return json.dumps(args)





