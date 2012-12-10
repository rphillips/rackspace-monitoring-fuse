#!/usr/bin/env python

import logging
import re
import json

logging.basicConfig()

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from rackspace_monitoring.types import Provider
from rackspace_monitoring.providers import get_driver

import libcloud.security
libcloud.security.VERIFY_SSL_CERT = False

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

class MaaSVNode(object):
    def __init__(self, obj, path):
        self.obj = obj
        self.path = path
        self._public_obj = self._get_public_obj()

    def get_obj(self):
        return self.obj

    def _get_public_obj(self):
        po = {}
        for attr in dir(self.obj):
            if not attr.startswith('__'):
                at = getattr(self.obj, attr)
                if type(at).__name__ in ['function', 'instancemethod',
                    'instance', 'RackspaceMonitoringDriver']:
                    continue
                po[attr] = str(at)
        return po

    def _serialize(self):
        return json.dumps(self._public_obj, indent=2)

    def read(self):
        return self._serialize()

    def length(self):
        return len(self._serialize())

class MaaSFile(MaaSVNode):
    def getattr(self):
        now = time()
        return dict(st_mode=(S_IFREG | 0600), st_ctime=now,
            st_mtime=now, st_atime=now, st_size=self.length(),
            st_nlink=2)

class MaaSDir(MaaSVNode):
    def getattr(self):
        now = time()
        return dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
            st_mtime=now, st_atime=now,
            st_nlink=2)

class MaaS(LoggingMixIn, Operations):
    ''

    def __init__(self):
        raxMon = get_driver(Provider.RACKSPACE)
        self.driver = raxMon(argv[2], argv[3])
        self.files = {}
        self.files['/'] = MaaSDir(None, '/')

    def chmod(self, path, mode):
        return 0

    def chown(self, path, uid, gid):
        return

    def create(self, path, mode):
        return 0

    def mkdir(self, path, mode):
        return

    def read(self, path, size, offset, fh):
        return self.files[path].read()

    def getattr(self, path, fh=None):
        if path not in self.files:
            raise FuseOSError(ENOENT)
        return self.files[path].getattr()

    def readdir(self, path, fh):
        if path == '/':
            entities = self.driver.list_entities()
            for en in entities:
                path = '/' + en.id
                self.files[path] = MaaSDir(en, path)
            return ['.', '..'] + [x.id for x in entities]

        matches = re.match(r'^/(en.+?)/(ch.+?)$', path)
        if matches:
            check = self.driver.get_check(matches.group(1), matches.group(2))
            self.files[path + '/attributes'] = MaaSFile(check, path)
            return ['.', '..'] + ['attributes']

        matches = re.match(r'^/(en.+?)$', path)
        if matches:
            checks = self.driver.list_checks(self.files[path].get_obj())
            for ch in checks:
                self.files[path + '/' + ch.id] = MaaSDir(ch, path)
            return ['.', '..'] + [x.id for x in checks]

        return ['.', '..']

    def readlink(self, path):
        return

    def rename(self, old, new):
        return

    def rmdir(self, path):
        return

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        return

    def truncate(self, path, length, fh=None):
        return

    def unlink(self, path):
        return

    def utimens(self, path, times=None):
        return

    def write(self, path, data, offset, fh):
        return


if __name__ == '__main__':
    if len(argv) != 4:
        print('usage: %s <mountpoint> <username> <token>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(MaaS(), argv[1], foreground=True)
