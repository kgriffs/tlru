# Copyright 2018 by Kurt Griffiths
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections
import logging
import struct
import time

import backoff
from csiphash import siphash24
from pecyn import packb, unpackb
from xxhash import xxh64

from .version import __version__  # NOQA


_log = logging.getLogger('tlru')


Level2RW = collections.namedtuple('Level2RW', ['r', 'w'])


def _backoff_on_exception():
    return backoff.on_exception(backoff.expo, Exception, max_tries=5)


class UnsupportedMediaType(Exception):
    def __init__(self, mtype):
        super().__init__('Unsupported media type: ' + mtype)

        self.mtype = mtype


class LRUDict(collections.MutableMapping):
    """
    Note that this is not thread-safe, since we assume it won't be
    used in such an environment. This lets us be a little more performant
    since we don't have to use a lock. Python is typically scaled via
    multiple processes and async I/O, so most apps should be able to
    safely use LRUDict (since LRUDict does not perform I/O, its methods
    will not be preempted by async libraries).

    On average, items will be valid for max_ttl/2 seconds (when max_ttl is
    specified; otherwise items will only be evicted via the LRU algorithm).
    """
    __slots__ = [
        '_lru_list',
        '_max_items',
        '_store',
        '_max_ttl',
    ]

    def __init__(self, max_items=128, max_ttl=None):
        if max_items < 1:
            raise ValueError('max_items must be >= 1')

        self._store = collections.OrderedDict()
        self._max_items = max_items
        self._max_ttl = max_ttl

    def __getitem__(self, key):
        tk = self._timed_key(key)

        try:
            # NOTE(kgriffs): Remove to ensure the new value is relocated
            value = self._store.pop(tk)
        except KeyError:
            raise KeyError(key)

        self._store[tk] = value
        return value

    def __setitem__(self, key, value):
        tk = self._timed_key(key)

        # NOTE(kgriffs): Remove to ensure the new value is relocated
        if tk in self._store:
            del self._store[tk]

        self._store[tk] = value

        if len(self._store) > self._max_items:
            self._store.popitem(last=False)

    def __delitem__(self, key):
        tk = self._timed_key(key)
        try:
            del self._store[tk]
        except KeyError:
            raise KeyError(key)

    def __iter__(self):
        if not self._max_ttl:
            for key in self._store:
                yield key
        else:
            current_time_slot = int(time.time() / self._max_ttl)

            for tk in self._store:
                ts_bytes = tk[-4:]
                time_slot = struct.unpack('<I', ts_bytes)[0]
                if time_slot == current_time_slot:
                    yield tk[:-5].decode('utf-8')

    def __len__(self):
        return len(self._store)

    def incr(self, key, by=1):
        tk = self._timed_key(key)

        new_value = self._store.pop(key, 0) + by
        self._store[tk] = new_value

        if len(self._store) > self._max_items:
            self._store.popitem(last=False)

        return new_value

    @property
    def size(self):
        return len(self._store)

    def items(self):
        # NOTE(kgriffs): Override default items() to avoid reordering
        #   them when they are accessed.
        if not self._max_ttl:
            for item in self._store.items():
                yield item
        else:
            current_time_slot = int(time.time() / self._max_ttl)

            for tk in self._store:
                ts_bytes = tk[-4:]
                time_slot = struct.unpack('<I', ts_bytes)[0]
                if time_slot == current_time_slot:
                    yield (tk[:-5], self._store[tk])

    iteritems = items

    def _timed_key(self, key):
        if not self._max_ttl:
            return key

        if not isinstance(key, bytes):
            key = key.encode('utf-8')

        time_slot = int(time.time() / self._max_ttl)

        # NOTE(kgriffs): Be explicit about the endianness in case we ever
        #   deploy to mixed architectures.
        ts_bytes = struct.pack('<I', time_slot)

        return key + b'\n' + ts_bytes


class CompositeCache:
    _LP = 'CompositeCache'

    __slots__ = [
        '_compression_thresholds',
        '_level1',
        '_level1_max_item_size',
        '_level1_max_items',
        '_level2',
        '_level2_max_item_size',
        '_logger',
        '_max_ttl',
        '_namespace',
    ]

    def __init__(
        self,
        namespace,
        max_ttl,
        level2rw,  # as an instance of Level2RW
        level1_max_items=256,
        level1_max_item_size=(4 * 2**10),
        level2_max_item_size=(1 * 2**20),

        # NOTE(kgriffs): Default of 4K helps constrain items to a single memory
        #   page, but it's TBD whether or not this is helpful.
        compression_threshold=(4 * 2**10),
    ):
        self._namespace = namespace.encode('utf-8')

        self._max_ttl = max_ttl
        self._level2 = level2rw

        self._level1_max_item_size = level1_max_item_size
        self._level2_max_item_size = level2_max_item_size
        self._compression_thresholds = (
            compression_threshold,
            level1_max_item_size,
            level2_max_item_size,
        )

        self._level1 = LRUDict(level1_max_items)

    def put(self, key, doc):
        hashed_key = self._hash_key(key)

        record = packb(doc)
        if any(len(record) >= t for t in self._compression_thresholds):
            # NOTE(kgriffs): It is better to take the time up front to
            #   compress and--as a result--hopefully be able to cache
            #   it, rather than not be able to cache it at all.
            record = packb(doc, compress=True)

        if len(record) <= self._level1_max_item_size:
            self._level1[hashed_key] = record

        if len(record) <= self._level2_max_item_size:
            try:
                self._l2_put(hashed_key, record)
            except Exception as ex:
                _log.warning('Error while putting item into the L2 cache', exc_info=ex)

    def get(self, key):
        hashed_key = self._hash_key(key)

        try:
            record = self._level1[hashed_key]
        except KeyError:
            try:
                record = self._l2_get(hashed_key)
            except Exception as ex:
                record = None
                _log.warning('Error while looking up item in the L2 cache', exc_info=ex)

            if record is not None:
                self._level1[hashed_key] = record

        if record is None:
            return None

        return unpackb(record)

    @_backoff_on_exception()
    def _l2_put(self, hashed_key, record):
        self._level2.w.set(hashed_key, record)

    @_backoff_on_exception()
    def _l2_get(self, hashed_key):
        return self._level2.r.get(hashed_key)

    def _hash_key(self, key):
        # NOTE(kgriffs): Be extremely cautious about changing this implementation,
        #   as it will result in invalidating any items in caches, etc. since
        #   they will now have a different key.

        if not isinstance(key, bytes):
            key = key.encode('utf-8')

        time_slot = int(time.time() / self._max_ttl)

        # NOTE(kgriffs): Be explicit about the endianness in case we ever
        #   deploy to mixed architectures.
        ts_bytes = struct.pack('<I', time_slot)

        # NOTE(kgriffs): Hash it to normalize the length; should be more
        #   optimal in terms of memory usage and traversal. Also, Redis
        #   seems to perform slightly better with pre-hashed keys.
        hash_input = self._namespace + b'\n' + key + b'\n' + ts_bytes

        # NOTE(kgriffs): This is about 2x as fast as sha256 and results in a
        #   smaller digest. Should be collision-resistant enough for
        #   use in cache keys (TBD).
        a = xxh64(hash_input).digest()
        b = siphash24(b'\x00' * 16, hash_input)
        digest = (a + b)

        return digest
