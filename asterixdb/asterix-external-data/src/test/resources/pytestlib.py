import json

def asterixInitialize():
    return None


def asterixDeinitialize():
    return None


def nextFrame(asterixArgs):
    args = {}
    args["given"] = asterixArgs[0]
    args["hello"] = "world"
    args["num"] = 1
    args["rec"] = {"nest":"ed","foo":"bar"}
    args["list"] = [1,2,3,4,5]
    return args





