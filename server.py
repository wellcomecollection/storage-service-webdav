# -*- coding: utf-8 -*-
# (c) 2009-2020 Martin Wendt and contributors; see WsgiDAV https://github.com/mar10/wsgidav
# Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
"""
Sample implementation of a DAV provider that provides a browsable,
multi-categorized resource tree.

Note that this is simply an example with no concrete real world benefit.
But it demonstrates some techniques to customize WsgiDAV.

Compared to a published file system, we have these main differences:

#. A resource like ``My doc 1`` has several attributes like ``key``,
   ``orga``, ``tags``, ``status``, ``description``.
   Also there may be a list of attached files.
#. These attributes are used to dynamically create a virtual hierarchy.
   For example, if ``status`` is ``draft``, a collection
   ``<share>/by_status/draft/`` is created and the resource is mapped to
   ``<share>/by_status/draft/My doc 1``.
#. The resource ``My doc 1`` is rendered as a collection, that contains
   some virtual descriptive files and the attached files.
#. The same resource may be referenced using different paths
   For example ``<share>/by_tag/cool/My doc 1``,
   ``<share>/by_tag/hot/My doc 1``, and ``<share>/by_key/1`` map to the same
   resource.
   Only the latter is considered the *real-path*, all others are
   *virtual-paths*.
#. The attributes are exposed as live properties, like "{virtres:}key",
   "{virtres:}tags", and "{virtres:}description".
   Some of them are even writable. Note that modifying an attribute may also
   change the dynamically created tree structure.
   For example changing "{virtres:}status" from 'draft' to 'published' will
   make the resource appear as ``<share>/by_status/published/My doc 1``.
#. This provider implements native delete/move/copy methods, to change the
   semantics of these operations for the virtual '/by_tag/' collection.
   For example issuing a DELETE on ``<share>/by_tag/cool/My doc 1`` will
   simply remove the 'cool' tag from that resource.
#. Virtual collections and artifacts cannot be locked.
   However a resource can be locked.
   For example locking ``<share>/by_tag/cool/My doc 1`` will also lock
   ``<share>/by_key/1``.
#. Some paths may be hidden, i.e. by_key is not browsable (but can be referenced)
   TODO: is this WebDAV compliant?

The *database* is a simple hard coded variable ``_resourceData``, that contains
a list of resource description dictionaries.

A resource is served as an collection, which is generated on-the-fly and
contains some virtual files (*artifacts*).

In general, a URL is interpreted like this::

  <share>/<category-type>/<category-key>/<resource-name>/<artifact-name>


An example layout::

    <share>/
        by_tag/
            cool/
                My doc 1/
                    .Info.html
                    .Info.txt
                    .Description.txt
                    MySpec.pdf
                    MySpec.doc
                My doc 2/
            hot/
                My doc 1/
                My doc 2/
            nice/
                My doc 2/
                My doc 3
        by_orga/
            development/
                My doc 3/
            marketing/
                My doc 1/
                My doc 2/
        by_status/
            draft/
                My doc 2
            published/
                My doc 1
                My doc 3
        by_key/
            1/
            2/
            3/

When accessed using WebDAV, the following URLs both return the same resource
'My doc 1'::

    <share>/by_tag/cool/My doc 1
    <share>/by_tag/hot/My doc 1
    <share>/by_key/1
"""
from wsgidav import compat, util
from wsgidav.dav_error import (
    DAVError,
    HTTP_FORBIDDEN,
    HTTP_INTERNAL_ERROR,
    PRECONDITION_CODE_ProtectedProperty,
)
from wsgidav.dav_provider import DAVCollection, DAVNonCollection, DAVProvider
from wsgidav.util import join_uri

import os
import stat


__docformat__ = "reStructuredText en"

_logger = util.get_module_logger(__name__)

BUFFER_SIZE = 8192

# ============================================================================
# Fake hierarchical repository
# ============================================================================
"""
This is a dummy 'database', that serves as an example source for the
VirtualResourceProvider.

All files listed in resPathList are expected to exist in FILE_FOLDER.
"""
FILE_FOLDER = r"c:\temp\virtfiles"

_resourceData = [
    {
        "key": "1",
        "title": "My doc 1",
        "orga": "development",
        "tags": ["cool", "hot"],
        "status": "draft",
        "description": "This resource contains two specification files.",
        "resPathList": [
            os.path.join(FILE_FOLDER, "MySpec.doc"),
            os.path.join(FILE_FOLDER, "MySpec.pdf"),
        ],
    },
    {
        "key": "2",
        "title": "My doc 2",
        "orga": "development",
        "tags": ["cool", "nice"],
        "status": "published",
        "description": "This resource contains one file.",
        "resPathList": [os.path.join(FILE_FOLDER, "My URS.doc")],
    },
    {
        "key": "3",
        "title": u"My doc (euro:\u20AC, uuml:��)".encode("utf8"),
        "orga": "marketing",
        "tags": ["nice"],
        "status": "published",
        "description": "Long text describing it",
        "resPathList": [os.path.join(FILE_FOLDER, "My URS.doc")],
    },
]


def _get_res_list_by_attr(attrName, attrVal):
    """"""
    assert attrName in RootCollection._visibleMemberNames
    if attrName == "by_status":
        return [data for data in _resourceData if data.get("status") == attrVal]
    elif attrName == "by_orga":
        resList = [data for data in _resourceData if data.get("orga") == attrVal]
    elif attrName == "by_tag":
        resList = [data for data in _resourceData if attrVal in data.get("tags")]
    return resList


def _get_res_by_key(key):
    """"""
    for data in _resourceData:
        if data["key"] == key:
            return data
    return None


# ============================================================================
#
# ============================================================================
from storage_service import _create_elastic_client as create_elastic_client

elastic_client = create_elastic_client()


class SpaceBrowser(DAVCollection):
    """
    Browse within a space in the storage service.
    """
    def __init__(self, path, environ, space):
        super().__init__(path, environ)
        self.space = space

    def get_display_info(self):
        return {"type": "Space", "space": self.space}

    def get_member_names(self):
        resp = elastic_client.search(
            index="storage_stage_bags",
            body={
  "query": {
    "bool": {
      "must": [
        {"term": {
          "space": {
            "value": self.space
          }
        }}
      ]
    }
  },
  "_source": "info.externalIdentifier",
  "size": 10000
}
        )
        hits = resp["hits"]["hits"]
        # https://stackoverflow.com/questions/9847288/is-it-possible-to-use-in-a-filename#comment12550838_9847306
        return sorted(h["_source"]["info"]["externalIdentifier"].replace("/", "\u29f8") for h in hits)

    def get_member(self, name):
        return ExternalIdentifierBrowser(join_uri(self.path, name), self.environ, space=self.space, externalIdentifier=name.replace("\u29f8", "/"))


import functools
from wellcome_storage_service import RequestsOAuthStorageServiceClient

storage_client = RequestsOAuthStorageServiceClient.from_path(api_url="https://api-stage.wellcomecollection.org/storage/v1")

@functools.lru_cache()
def get_bag(space, externalIdentifier, version):
    return storage_client.get_bag(space, externalIdentifier, version)


class ExternalIdentifierBrowser(DAVCollection):
    """
    Browse within the versions of a bag in the storage service.
    """
    def __init__(self, path, environ, space, externalIdentifier):
        super().__init__(path, environ)
        self.space = space
        self.externalIdentifier = externalIdentifier

    def get_display_info(self):
        return {"type": "ExternalIdentifier", "space": self.space, "externalIdentifier": self.externalIdentifier}

    def get_member_names(self):
        resp = elastic_client.get(
            index="storage_stage_bags",
            id=f"{self.space}/{self.externalIdentifier}",
            params={
                "_source": "version"
            }
        )
        version = resp["_source"]["version"]

        return [f"v{v}" for v in range(1, version + 1)]

    def get_member(self, bagVersion):
        return BagPathBrowser(path=join_uri(self.path, bagVersion), environ=self.environ, space=self.space, externalIdentifier=self.externalIdentifier, bagVersion=bagVersion.replace("._", ""))


class BagPathBrowser(DAVCollection):
    """
    Browse within a bag.
    """
    def __init__(self, path, environ, space, externalIdentifier, bagVersion, bagPath=""):
        super().__init__(path, environ)
        self.space = space
        self.externalIdentifier = externalIdentifier
        self.bagVersion = bagVersion
        self.bagPath = bagPath
        self.bag = get_bag(space, externalIdentifier, bagVersion)

        # TODO: This is a bit fiddly
        self.members = {
            "files": {},
            "directories": set(),
        }

        import os

        for f in self.bag["manifest"]["files"] + self.bag["tagManifest"]["files"]:
            if f["name"].count("/") == bagPath.count("/"):
                self.members["files"][os.path.basename(f["name"])] = f["path"]
            elif not f['name'].startswith(bagPath):
                continue
            else:
                self.members["directories"].add(f["name"].split("/", bagPath.count("/") + 1)[-2])

    def get_display_info(self):
        return {"type": "ExternalIdentifier", "space": self.space, "externalIdentifier": self.externalIdentifier, "version": self.bagVersion, "bagPath": self.bagPath}

    def get_member_names(self):
        return sorted(list(self.members["directories"]) + list(self.members["files"]))

    def get_member(self, name):
        if name.startswith("._"):
            return None

        if name in self.members["directories"]:
            return BagPathBrowser(
                path=join_uri(self.path, name), environ=self.environ, space=self.space, externalIdentifier=self.externalIdentifier, bagVersion=self.bagVersion,
                bagPath=os.path.join(self.bagPath, name) + "/"
            )
        else:
            return VirtualBagFile(
                path=join_uri(self.path, name), environ=self.environ,
                s3_location=self.bag["location"],
                file_path=self.members["files"][name]
            )

from aws import get_aws_client

s3 = get_aws_client("s3", role_arn="arn:aws:iam::975596993436:role/storage-read_only")


class VirtualBagFile(DAVNonCollection):
    def __init__(self, path, environ, s3_location, file_path):
        super().__init__(path, environ)
        self.s3_location = s3_location
        self.file_path = file_path

    @property
    def s3_bucket(self):
        return self.s3_location["bucket"]

    @property
    def s3_key(self):
        return os.path.join(self.s3_location["path"], self.file_path)

    def get_content_length(self):
        return s3.head_object(Bucket=self.s3_bucket, Key=self.s3_key)["ContentLength"]

    def get_content_type(self):
        return util.guess_mime_type(self.file_path)

    def get_content(self):
        return s3.get_object(Bucket=self.s3_bucket, Key=self.s3_key)["Body"]
        # mime = self.get_content_type()
        # GC issue 57: always store as binary
        # if mime.startswith("text"):
        #     return open(self.file_path, "r", BUFFER_SIZE)
        # return open(self.file_path, "rb", BUFFER_SIZE)



class CategoryTypeCollection(DAVCollection):
    """Resolve '/catType' URLs, for example '/by_tag'."""

    def __init__(self, path, environ):
        DAVCollection.__init__(self, path, environ)

    def get_display_info(self):
        return {"type": "Category type"}

    def get_member_names(self):
        names = []
        for data in _resourceData:
            if self.name == "by_status":
                if not data["status"] in names:
                    names.append(data["status"])
            elif self.name == "by_orga":
                if not data["orga"] in names:
                    names.append(data["orga"])
            elif self.name == "by_tag":
                for tag in data["tags"]:
                    if tag not in names:
                        names.append(tag)

        names.sort()
        return names

    def get_member(self, name):
        if self.name == "by_key":
            data = _get_res_by_key(name)
            if data:
                return VirtualResource(join_uri(self.path, name), self.environ, data)
            else:
                return None
        return CategoryCollection(join_uri(self.path, name), self.environ, self.name)


class CategoryCollection(DAVCollection):
    """Resolve '/catType/cat' URLs, for example '/by_tag/cool'."""

    def __init__(self, path, environ, catType):
        DAVCollection.__init__(self, path, environ)
        self.catType = catType

    def get_display_info(self):
        return {"type": "Category"}

    def get_member_names(self):
        names = [
            data["title"] for data in _get_res_list_by_attr(self.catType, self.name)
        ]
        names.sort()
        return names

    def get_member(self, name):
        for data in _get_res_list_by_attr(self.catType, self.name):
            if data["title"] == name:
                return VirtualResource(join_uri(self.path, name), self.environ, data)
        return None


# ============================================================================
# VirtualResource
# ============================================================================
class VirtualResource(DAVCollection):
    """A virtual 'resource', displayed as a collection of artifacts and files."""

    _artifactNames = (
        ".Info.txt",
        ".Info.html",
        ".Description.txt",
        # ".Admin.html",
    )
    _supportedProps = [
        "{virtres:}key",
        "{virtres:}title",
        "{virtres:}status",
        "{virtres:}orga",
        "{virtres:}tags",
        "{virtres:}description",
    ]

    def __init__(self, path, environ, data):
        DAVCollection.__init__(self, path, environ)
        self.data = data

    def get_display_info(self):
        return {"type": "Virtual Resource"}

    def get_member_names(self):
        names = list(self._artifactNames)
        for f in self.data["resPathList"]:
            name = os.path.basename(f)
            names.append(name)
        return names

    def get_member(self, name):
        if name in self._artifactNames:
            return VirtualArtifact(join_uri(self.path, name), self.environ, self.data)
        for file_path in self.data["resPathList"]:
            fname = os.path.basename(file_path)
            if fname == name:
                return VirtualResFile(
                    join_uri(self.path, name), self.environ, self.data, file_path
                )
        return None

    def handle_delete(self):
        """Change semantic of DELETE to remove resource tags."""
        # DELETE is only supported for the '/by_tag/' collection
        if "/by_tag/" not in self.path:
            raise DAVError(HTTP_FORBIDDEN)
        # path must be '/by_tag/<tag>/<resname>'
        catType, tag, _rest = util.save_split(self.path.strip("/"), "/", 2)
        assert catType == "by_tag"
        assert tag in self.data["tags"]
        self.data["tags"].remove(tag)
        return True  # OK

    def handle_copy(self, dest_path, depth_infinity):
        """Change semantic of COPY to add resource tags."""
        # destPath must be '/by_tag/<tag>/<resname>'
        if "/by_tag/" not in dest_path:
            raise DAVError(HTTP_FORBIDDEN)
        catType, tag, _rest = util.save_split(dest_path.strip("/"), "/", 2)
        assert catType == "by_tag"
        if tag not in self.data["tags"]:
            self.data["tags"].append(tag)
        return True  # OK

    def handle_move(self, dest_path):
        """Change semantic of MOVE to change resource tags."""
        # path and destPath must be '/by_tag/<tag>/<resname>'
        if "/by_tag/" not in self.path:
            raise DAVError(HTTP_FORBIDDEN)
        if "/by_tag/" not in dest_path:
            raise DAVError(HTTP_FORBIDDEN)
        catType, tag, _rest = util.save_split(self.path.strip("/"), "/", 2)
        assert catType == "by_tag"
        assert tag in self.data["tags"]
        self.data["tags"].remove(tag)
        catType, tag, _rest = util.save_split(dest_path.strip("/"), "/", 2)
        assert catType == "by_tag"
        if tag not in self.data["tags"]:
            self.data["tags"].append(tag)
        return True  # OK

    def get_ref_url(self):
        refPath = "/by_key/%s" % self.data["key"]
        return compat.quote(self.provider.share_path + refPath)

    def get_property_names(self, is_allprop):
        """Return list of supported property names in Clark Notation.

        See DAVResource.get_property_names()
        """
        # Let base class implementation add supported live and dead properties
        propNameList = super(VirtualResource, self).get_property_names(is_allprop)
        # Add custom live properties (report on 'allprop' and 'propnames')
        propNameList.extend(VirtualResource._supportedProps)
        return propNameList

    def get_property_value(self, name):
        """Return the value of a property.

        See get_property_value()
        """
        # Supported custom live properties
        if name == "{virtres:}key":
            return self.data["key"]
        elif name == "{virtres:}title":
            return self.data["title"]
        elif name == "{virtres:}status":
            return self.data["status"]
        elif name == "{virtres:}orga":
            return self.data["orga"]
        elif name == "{virtres:}tags":
            # 'tags' is a string list
            return ",".join(self.data["tags"])
        elif name == "{virtres:}description":
            return self.data["description"]
        # Let base class implementation report live and dead properties
        return super(VirtualResource, self).get_property_value(name)

    def set_property_value(self, name, value, dry_run=False):
        """Set or remove property value.

        See DAVResource.set_property_value()
        """
        if value is None:
            # We can never remove properties
            raise DAVError(HTTP_FORBIDDEN)
        if name == "{virtres:}tags":
            # value is of type etree.Element
            self.data["tags"] = value.text.split(",")
        elif name == "{virtres:}description":
            # value is of type etree.Element
            self.data["description"] = value.text
        elif name in VirtualResource._supportedProps:
            # Supported property, but read-only
            raise DAVError(
                HTTP_FORBIDDEN, err_condition=PRECONDITION_CODE_ProtectedProperty
            )
        else:
            # Unsupported property
            raise DAVError(HTTP_FORBIDDEN)
        # Write OK
        return


# ============================================================================
# _VirtualNonCollection classes
# ============================================================================
class _VirtualNonCollection(DAVNonCollection):
    """Abstract base class for all non-collection resources."""

    def __init__(self, path, environ):
        DAVNonCollection.__init__(self, path, environ)

    def get_content_length(self):
        return None

    def get_content_type(self):
        return None

    def get_creation_date(self):
        return None

    def get_display_name(self):
        return self.name

    def get_display_info(self):
        raise NotImplementedError

    def get_etag(self):
        return None

    def get_last_modified(self):
        return None

    def support_ranges(self):
        return False


#    def handle_delete(self):
#        raise DAVError(HTTP_FORBIDDEN)
#    def handle_move(self, destPath):
#        raise DAVError(HTTP_FORBIDDEN)
#    def handle_copy(self, destPath, depthInfinity):
#        raise DAVError(HTTP_FORBIDDEN)


# ============================================================================
# VirtualArtifact
# ============================================================================
class VirtualArtifact(_VirtualNonCollection):
    """A virtual file, containing resource descriptions."""

    def __init__(self, path, environ, data):
        #        assert name in _artifactNames
        _VirtualNonCollection.__init__(self, path, environ)
        self.data = data

    def get_content_length(self):
        return len(self.get_content().read())

    def get_content_type(self):
        if self.name.endswith(".txt"):
            return "text/plain"
        return "text/html"

    def get_display_info(self):
        return {"type": "Virtual info file"}

    def prevent_locking(self):
        return True

    def get_ref_url(self):
        refPath = "/by_key/%s/%s" % (self.data["key"], self.name)
        return compat.quote(self.provider.share_path + refPath)

    def get_content(self):
        fileLinks = [
            "<a href='%s'>%s</a>\n" % (os.path.basename(f), f)
            for f in self.data["resPathList"]
        ]
        dict = self.data.copy()
        dict["fileLinks"] = ", ".join(fileLinks)
        if self.name == ".Info.html":
            html = (
                """\
            <html><head>
            <title>%(title)s</title>
            </head><body>
            <h1>%(title)s</h1>
            <table>
            <tr>
                <td>Description</td>
                <td>%(description)s</td>
            </tr><tr>
                <td>Status</td>
                <td>%(status)s</td>
            </tr><tr>
                <td>Tags</td>
                <td>%(tags)s</td>
            </tr><tr>
                <td>Orga unit</td>
                <td>%(orga)s</td>
            </tr><tr>
                <td>Files</td>
                <td>%(fileLinks)s</td>
            </tr><tr>
                <td>Key</td>
                <td>%(key)s</td>
            </tr>
            </table>
            <p>This is a virtual WsgiDAV resource called '%(title)s'.</p>
            </body></html>"""
                % dict
            )
        elif self.name == ".Info.txt":
            lines = [
                self.data["title"],
                "=" * len(self.data["title"]),
                self.data["description"],
                "",
                "Status: %s" % self.data["status"],
                "Orga:   %8s" % self.data["orga"],
                "Tags:   '%s'" % "', '".join(self.data["tags"]),
                "Key:    %s" % self.data["key"],
            ]
            html = "\n".join(lines)
        elif self.name == ".Description.txt":
            html = self.data["description"]
        else:
            raise DAVError(HTTP_INTERNAL_ERROR, "Invalid artifact '%s'" % self.name)
        return compat.BytesIO(compat.to_bytes(html))


# ============================================================================
# VirtualResFile
# ============================================================================
class VirtualResFile(_VirtualNonCollection):
    """Represents an existing file, that is a member of a VirtualResource."""

    def __init__(self, path, environ, data, file_path):
        if not os.path.exists(file_path):
            _logger.error("VirtualResFile(%r) does not exist." % file_path)
        _VirtualNonCollection.__init__(self, path, environ)
        self.data = data
        self.file_path = file_path

    def get_content_length(self):
        statresults = os.stat(self.file_path)
        return statresults[stat.ST_SIZE]

    def get_content_type(self):
        if not os.path.isfile(self.file_path):
            return "text/html"
        #        (mimetype, _mimeencoding) = mimetypes.guess_type(self.file_path)
        #        if not mimetype:
        #            mimetype = "application/octet-stream"
        #        return mimetype
        return util.guess_mime_type(self.file_path)

    def get_creation_date(self):
        statresults = os.stat(self.file_path)
        return statresults[stat.ST_CTIME]

    def get_display_info(self):
        return {"type": "Content file"}

    def get_last_modified(self):
        statresults = os.stat(self.file_path)
        return statresults[stat.ST_MTIME]

    def get_ref_url(self):
        refPath = "/by_key/%s/%s" % (self.data["key"], os.path.basename(self.file_path))
        return compat.quote(self.provider.share_path + refPath)

    def get_content(self):
        # mime = self.get_content_type()
        # GC issue 57: always store as binary
        # if mime.startswith("text"):
        #     return open(self.file_path, "r", BUFFER_SIZE)
        return open(self.file_path, "rb", BUFFER_SIZE)


# ============================================================================
# VirtualResourceProvider
# ============================================================================
class VirtualResourceProvider(DAVProvider):
    """
    DAV provider that serves a VirtualResource derived structure.
    """

    def __init__(self):
        super(VirtualResourceProvider, self).__init__()
        self.resourceData = _resourceData

    def get_resource_inst(self, path, environ):
        """Return _VirtualResource object for path.

        path is expected to be
            categoryType/category/name/artifact
        for example:
            'by_tag/cool/My doc 2/info.html'

        See DAVProvider.get_resource_inst()
        """
        _logger.info("get_resource_inst('%s')" % path)
        self._count_get_resource_inst += 1
        from dav_provider import TopLevelBrowser
        root = TopLevelBrowser(environ)
        return root.resolve("", path)
