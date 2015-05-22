import tweetbook_bootstrap
import json

from bottle import route, run, template, static_file, request

import requests
from requests import ConnectionError, HTTPError

# Core Routing
@route('/')
def demo():
    return template('tweetbook')

@route('/static/<filename:path>')
def send_static(filename):
    return static_file(filename, root='static')

# API Helpers
def build_response(endpoint, data):
    api_endpoint = "http://localhost:19002/" + endpoint

    http_header = {
        'Accept': 'application/json'
    }
   
    try:
        response = requests.get(api_endpoint, params=data.json, headers=http_header)
        return json.dumps({'results' : response.json()})
    except (ValueError):
        return json.dumps({})
    except (ConnectionError, HTTPError):
        # This exception will stop the server. Not optimal behavior.
        print "Encountered connection error; stopping execution"
        sys.exit(1)

# API Endpoints
@route('/query', method='POST')
def run_asterix_query():
    return (build_response("query", request))
    
@route('/query/status', method='POST')
def run_asterix_query_status():
    return (build_response("query/status", request))

@route('/query/result', method='POST')
def run_asterix_query_result():
    return (build_response("query/result", request))

@route('/ddl', method='POST')
def run_asterix_ddl():
    return (build_response("ddl", request))

@route('/update', method='POST')
def run_asterix_update():
    return (build_response("update", request))
    
res = tweetbook_bootstrap.bootstrap()
run(host='localhost', port=8080, debug=True)
