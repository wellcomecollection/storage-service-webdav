import logging
import sys

from cheroot import wsgi
from wsgidav.dir_browser import WsgiDavDirBrowser
from wsgidav.error_printer import ErrorPrinter
from wsgidav.request_resolver import RequestResolver
from wsgidav.wsgidav_app import WsgiDAVApp


from dav_provider import StorageServiceProvider


logger = logging.getLogger("wsgidav")

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


config = {
    "provider_mapping": {"/": StorageServiceProvider()},
    "verbose": 1,
    "middleware_stack": [ErrorPrinter, WsgiDavDirBrowser, RequestResolver],
}


if __name__ == "__main__":
    app = WsgiDAVApp(config)

    server_args = {
        "bind_addr": ("localhost", 8875),
        "wsgi_app": app,
    }
    server = wsgi.Server(**server_args)
    server.start()
