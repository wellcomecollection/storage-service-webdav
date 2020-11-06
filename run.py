from cheroot import wsgi
from wsgidav.wsgidav_app import WsgiDAVApp
from dav_provider import StorageServiceProvider

from wsgidav.debug_filter import WsgiDavDebugFilter
from wsgidav.dir_browser import WsgiDavDirBrowser
from wsgidav.error_printer import ErrorPrinter
from wsgidav.http_authenticator import HTTPAuthenticator
from wsgidav.request_resolver import RequestResolver

import logging

# Logging should be initialized some way, e.g.:
# logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger("wsgidav")
logger.propagate = True
logger.setLevel(logging.INFO)

import sys

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)



config = {
    "host": "0.0.0.0",
    "port": 8080,
    "provider_mapping": {
        "/": StorageServiceProvider(),
        },
    "verbose": 1,
    # https://github.com/mar10/wsgidav/blob/2e375551f1961380d7afd2cbcf3bef32bb98b8d7/wsgidav/default_conf.py#L47-L53
    # but without HTTPAuthenticator
    "middleware_stack": [
        WsgiDavDebugFilter,
        ErrorPrinter,
        # HTTPAuthenticator,
        WsgiDavDirBrowser,  # configured under dir_browser option (see below)
        RequestResolver,  # this must be the last middleware item
    ],

    }

app = WsgiDAVApp(config)

server_args = {
    "bind_addr": (config["host"], config["port"]),
    "wsgi_app": app,
    }
server = wsgi.Server(**server_args)
server.start()
