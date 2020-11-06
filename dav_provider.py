import os

from wsgidav.dav_provider import DAVCollection, DAVNonCollection, DAVProvider
from wsgidav import util

from aws import get_aws_client
from storage_service import (
    get_bag,
    get_external_identifiers_in_space,
    get_latest_version_of_bag,
    get_list_of_spaces,
)


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
        return SpaceBrowser(
            path=util.join_uri(self.path, space), environ=self.environ, space=space
        )


class SpaceBrowser(DAVCollection):
    """
    Browse within a space in the storage service.

    This resolves requests of the form '/{space}/'.
    """

    def __init__(self, path, environ, space):
        super().__init__(path, environ)
        self.space = space

    def get_display_info(self):
        return {"type": "Space", "space": self.space}

    def get_member_names(self):
        external_identifiers = get_external_identifiers_in_space(space=self.space)

        # We want these externalIdentifiers to appear as a single string, not
        # a series of nested folders.  Cheat a little -- replace slashes with
        # Unicode lookalikes to get around this.
        # See https://stackoverflow.com/a/9847306/1558022#comment12550838_9847306
        return sorted(ext_id.replace("/", "\u29F8") for ext_id in external_identifiers)

    def get_member(self, name):
        return ExternalIdentifierBrowser(
            path=util.join_uri(self.path, name),
            environ=self.environ,
            space=self.space,
            # Remember to un-mangle the slashes before going to the next level down.
            externalIdentifier=name.replace("\u29F8", "/"),
        )


class ExternalIdentifierBrowser(DAVCollection):
    """
    Browse within the versions of a given (space, externalIdentifier) pair.

    This resolves requests of the form '/{space}/{externalIdentifier}'.
    """

    def __init__(self, path, environ, space, externalIdentifier):
        super().__init__(path, environ)
        self.space = space
        self.externalIdentifier = externalIdentifier

    def get_display_info(self):
        return {
            "type": "ExternalIdentifier",
            "space": self.space,
            "externalIdentifier": self.externalIdentifier,
        }

    def get_member_names(self):
        latest_version = get_latest_version_of_bag(
            space=self.space, externalIdentifier=self.externalIdentifier
        )

        # Note: this assumes that we've stored every version v1 ... vN.
        # This may not always be true: in particular, there are gaps in the staging
        # service.  This is good enough for an experiment.
        return [f"v{v}" for v in range(1, latest_version + 1)]

    def get_member(self, bagVersion):
        return BagDirectoryBrowser(
            path=util.join_uri(self.path, bagVersion),
            environ=self.environ,
            space=self.space,
            externalIdentifier=self.externalIdentifier,
            bagVersion=bagVersion,
        )


class BagDirectoryBrowser(DAVCollection):
    """
    Browse within a bag.
    """

    def __init__(
        self, path, environ, space, externalIdentifier, bagVersion, pathPrefix=""
    ):
        super().__init__(path, environ)
        self.space = space
        self.externalIdentifier = externalIdentifier
        self.bagVersion = bagVersion
        self.pathPrefix = pathPrefix
        self.bag = get_bag(space, externalIdentifier, bagVersion)

        # TODO: This is meant to find all the files/directories within this
        # particular directory of the bag.  This code is fiddly and could be
        # tested or improved.
        self.members = {
            "files": {},
            "directories": set(),
        }

        for f in self.bag["manifest"]["files"] + self.bag["tagManifest"]["files"]:
            # If you have the same number of slashes as the prefix, you must
            # be a file in the same directory.
            #
            #   e.g. pathPrefix = data/objects/
            #        f["name"]  = data/objects/cat.jpg
            #
            if f["name"].count("/") == pathPrefix.count("/"):
                self.members["files"][os.path.basename(f["name"])] = f["path"]

            # If this file starts with the pathPrefix, but has more slashes, then
            # it must be in some subdirectory.
            #
            #   e.g. pathPrefix = data/objects/
            #        f["name"]  = data/objects/images/cat.jpg
            #
            elif f["name"].startswith(pathPrefix):
                self.members["directories"].add(
                    f["name"].split("/", pathPrefix.count("/") + 1)[-2]
                )

            # Otherwise, this file should be in some completely different bit of
            # the bag.
            #
            #   e.g. pathPrefix = data/objects/
            #        f["name"]  = data/logs/fileIdentification.log
            #
            else:
                assert not f["name"].startswith(pathPrefix)

    def get_display_info(self):
        return {
            "type": "ExternalIdentifier",
            "space": self.space,
            "externalIdentifier": self.externalIdentifier,
            "version": self.bagVersion,
            "bagPath": self.bagPath,
        }

    def get_member_names(self):
        return sorted(list(self.members["directories"]) + list(self.members["files"]))

    def get_member(self, name):
        if name in self.members["directories"]:
            return BagDirectoryBrowser(
                path=util.join_uri(self.path, name),
                environ=self.environ,
                space=self.space,
                externalIdentifier=self.externalIdentifier,
                bagVersion=self.bagVersion,
                pathPrefix=os.path.join(self.pathPrefix, name) + "/",
            )
        else:
            return VirtualBagFile(
                path=util.join_uri(self.path, name),
                environ=self.environ,
                s3_location=self.bag["location"],
                bag_path=self.members["files"][name],
            )


class VirtualBagFile(DAVNonCollection):
    s3 = get_aws_client(
        "s3", role_arn="arn:aws:iam::975596993436:role/storage-read_only"
    )

    def __init__(self, path, environ, s3_location, bag_path):
        super().__init__(path, environ)
        self.s3_location = s3_location
        self.bag_path = bag_path

    @property
    def s3_bucket(self):
        return self.s3_location["bucket"]

    @property
    def s3_key(self):
        return os.path.join(self.s3_location["path"], self.bag_path)

    def get_content_length(self):
        head_resp = self.s3.head_object(Bucket=self.s3_bucket, Key=self.s3_key)
        return head_resp["ContentLength"]

    def get_content_type(self):
        return util.guess_mime_type(self.bag_path)

    def get_content(self):
        return self.s3.get_object(Bucket=self.s3_bucket, Key=self.s3_key)["Body"]


class StorageServiceProvider(DAVProvider):
    """
    DAV provider that provides a view into the Wellcome storage service.
    """

    def __init__(self):
        super().__init__()

    def get_resource_inst(self, path, environ):
        # Reduce the amount of noise from the macOS Finder requesting things
        # that don't exist/make sense.
        if os.path.basename(path).startswith("."):
            return None

        logger = util.get_module_logger(__name__)
        logger.info(f"get_resource_inst({path!r})")

        root = TopLevelBrowser(environ)
        return root.resolve("", path)
