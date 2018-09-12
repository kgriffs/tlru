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


class L2TestCache:
    def __init__(self):
        self._store = {}

    def set(self, key, value):
        self._store[key] = value

    def get(self, key):
        return self._store.get(key)

    def incr(self, key):
        try:
            self._store[key] += 1
        except KeyError:
            self._store[key] = 1

        return self._store[key]

    @classmethod
    def create_level2rw(cls):
        cache = cls()
        return Level2RW(cache, cache)


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

    They key is used as-is to lookup values an internal dict. It may be
    appended with a timestamp to provide max-ttl support. Regardless,
    this means the caller should pre-hash keys if they are huge, to reduce
    memory usage and to potentially improve lookup time.
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

    def __contains__(self, key):
        """Allows "peeking" to see if the cache contains an item, without changing LRU status."""
        return self._timed_key(key) in self._store

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

    def remove(self, key):
        """Like del my_lru[my_key] except doesn't raise KeyError."""
        tk = self._timed_key(key)
        try:
            del self._store[tk]
        except KeyError:
            pass

    def incr(self, key, by=1):
        tk = self._timed_key(key)

        new_value = self._store.pop(tk, 0) + by
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


class CompositeCacheNOOP:
    def put(self, key, doc):
        pass

    def get(self, key):
        return None

    def put_int64(self, key, number):
        pass

    def get_int64(self, key):
        return None


class CompositeCache:
    _LP = 'CompositeCache'

    __slots__ = [
        '_auto_compress',
        '_compression_thresholds',
        '_level1',
        '_level1_max_item_size',
        '_level1_max_items',
        '_level2',
        '_level2_max_item_size',
        '_max_ttl',
        '_namespace',
        '_negative_ttl_lru',
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
        auto_compress=None,

        # TODO(kgriffs): This works better when you can provide a "cache fill"
        #   callable to use on cache miss.
        negative_ttl=None,

        # NOTE(kgriffs): When provided, this will override max_ttl for the
        #   L1 cache. It must be less than max_ttl if set.
        level1_max_ttl=None,
    ):
        # NOTE(kgriffs): Enforce "least surprise" semantics.
        if level1_max_ttl is not None and level1_max_ttl >= max_ttl:
            raise ValueError('level1_max_ttl must be less than max_ttl')

        self._namespace = namespace.encode('utf-8')
        self._auto_compress = auto_compress if auto_compress is not None else True

        self._max_ttl = max_ttl

        if negative_ttl:
            self._negative_ttl_lru = LRUDict(level1_max_items, negative_ttl)
        else:
            self._negative_ttl_lru = None

        self._level2 = level2rw

        self._level1_max_item_size = level1_max_item_size
        self._level2_max_item_size = level2_max_item_size
        self._compression_thresholds = (
            compression_threshold,
            level1_max_item_size,
            level2_max_item_size,
        )

        self._level1 = LRUDict(level1_max_items, level1_max_ttl)

    def put(self, key, doc):
        hashed_key = self._hash_key(key)

        record = packb(doc)
        if self._auto_compress and any(len(record) >= t for t in self._compression_thresholds):
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

        if self._negative_ttl_lru:
            self._negative_ttl_lru.remove(hashed_key)

    def get(self, key):
        hashed_key = self._hash_key(key)

        if self._negative_ttl_lru and hashed_key in self._negative_ttl_lru:
            return None

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
            if self._negative_ttl_lru:
                self._negative_ttl_lru[hashed_key] = True

            return None

        return unpackb(record)

    def put_int64(self, key, number):
        if number is None:
            raise ValueError('number may not be None')

        hashed_key = self._hash_key(key)

        self._level1[hashed_key] = number

        record = str(number).encode()
        try:
            self._l2_put(hashed_key, record)
        except Exception as ex:
            _log.warning('Error while putting item into the L2 cache', exc_info=ex)

        if self._negative_ttl_lru:
            self._negative_ttl_lru.remove(hashed_key)

    def get_int64(self, key):
        hashed_key = self._hash_key(key)

        if self._negative_ttl_lru and hashed_key in self._negative_ttl_lru:
            return None

        number = None

        try:
            number = self._level1[hashed_key]
        except KeyError:
            try:
                record = self._l2_get(hashed_key)
            except Exception as ex:
                _log.warning('Error while looking up item in the L2 cache', exc_info=ex)
            else:
                number = int(record)
                self._level1[hashed_key] = number

        if number is None:
            if self._negative_ttl_lru:
                self._negative_ttl_lru[hashed_key] = True

        return number

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


class Level2Counter:
    _LP = 'Level2Counter'

    __slots__ = [
        '_level2',
        '_max_ttl',
        '_namespace',
    ]

    def __init__(
        self,
        namespace,
        max_ttl,
        level2,  # Must implement an incr(key) method on base-10 number string values
    ):
        self._namespace = namespace.encode('utf-8')
        self._max_ttl = max_ttl
        self._level2 = level2

    def put(self, key, number):
        if number is None:
            raise ValueError('number may not be None')

        hashed_key = self._hash_key(key)

        try:
            self._l2_put(hashed_key, str(number).encode())
        except Exception as ex:
            _log.warning('Error while putting item into the L2 cache', exc_info=ex)

    def get(self, key):
        hashed_key = self._hash_key(key)

        number = None

        try:
            record = self._l2_get(hashed_key)
        except Exception as ex:
            _log.warning('Error while looking up item in the L2 cache', exc_info=ex)
        else:
            number = int(record)

        return number

    def incr(self, key):
        hashed_key = self._hash_key(key)

        number = 1

        try:
            number = self._l2_incr(hashed_key)
        except Exception as ex:
            _log.warning(
                'Error while incrementing item in the L2 cache; returning default value (1).',
                exc_info=ex,
            )

        return number

    @_backoff_on_exception()
    def _l2_put(self, hashed_key, record):
        self._level2.set(hashed_key, record)

    @_backoff_on_exception()
    def _l2_get(self, hashed_key):
        return self._level2.get(hashed_key)

    @_backoff_on_exception()
    def _l2_incr(self, hashed_key):
        return self._level2.incr(hashed_key)

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
