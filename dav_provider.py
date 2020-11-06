from storage_service import get_list_of_spaces

from wsgidav.dav_provider import DAVCollection
from wsgidav import util


class TopLevelBrowser(DAVCollection):
    """
    Browse the top-level spaces of the storage service.

    This resolves top-level requests '/'.
    """
    def __init__(self, environ):
        super().__init__("/", environ)

    def get_member_names(self):
        return sorted(get_list_of_spaces())

    def get_member(self, space):
        from server import SpaceBrowser
        return SpaceBrowser(
            path=util.join_uri(self.path, space),
            environ=self.environ,
            space=space
        )
