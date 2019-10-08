#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A module for caching state reads/writes in Beam applications."""
from __future__ import absolute_import

import collections
import logging
from threading import RLock

from apache_beam.metrics import monitoring_infos

# A list of all registered metrics
ALL_METRICS = set()


def counter_hit_miss(total_name, hit_name, miss_name):
  """Decorator for counting function calls and whether
     the return value equals None (=miss) or not (=hit)."""
  ALL_METRICS.update([total_name, hit_name, miss_name])

  def decorator(function):

    def reporter(self, *args, **kwargs):
      value = function(self, *args, **kwargs)
      self._metrics[total_name] += 1
      if value is None:
        self._metrics[miss_name] += 1
      else:
        self._metrics[hit_name] += 1
      return value

    return reporter

  return decorator


def counter(metric_name):
  """Decorator for counting function calls."""
  ALL_METRICS.add(metric_name)

  def decorator(function):

    def reporter(self, *args, **kwargs):
      self._metrics[metric_name] += 1
      return function(self, *args, **kwargs)

    return reporter

  return decorator


class StateCache(object):
  """ Cache for Beam state access, scoped by state key and cache_token.
      Assumes a bag state implementation.

  For a given state_key, caches a (cache_token, value) tuple and allows to
    a) read from the cache,
           if the currently stored cache_token matches the provided
    a) write to the cache,
           storing the new value alongside with a cache token
    c) append to the currently cache item (extend),
           if the currently stored cache_token matches the provided
    c) empty a cached element (clear),
           if the currently stored cache_token matches the provided
    d) evict a cached element (evict)

  The operations on the cache are thread-safe for use by multiple workers.

  :arg max_entries The maximum number of entries to store in the cache.
  TODO Memory-based caching: https://issues.apache.org/jira/browse/BEAM-8297
  """

  def __init__(self, max_entries):
    logging.info('Creating state cache with size %s', max_entries)
    self._cache = self.LRUCache(max_entries, (None, None))
    self._lock = RLock()
    self._metrics = collections.defaultdict(int)

  @counter_hit_miss("get", "hit", "miss")
  def get(self, state_key, cache_token):
    assert cache_token and self.is_cache_enabled()
    with self._lock:
      token, value = self._cache.get(state_key)
    return value if token == cache_token else None

  @counter("put")
  def put(self, state_key, cache_token, value):
    assert cache_token and self.is_cache_enabled()
    with self._lock:
      return self._cache.put(state_key, (cache_token, value))

  @counter("extend")
  def extend(self, state_key, cache_token, elements):
    assert cache_token and self.is_cache_enabled()
    with self._lock:
      token, value = self._cache.get(state_key)
      if token in [cache_token, None]:
        if value is None:
          value = []
        value.extend(elements)
        self._cache.put(state_key, (cache_token, value))
      else:
        # Discard cached state if tokens do not match
        self.evict(state_key)

  @counter("clear")
  def clear(self, state_key, cache_token):
    assert cache_token and self.is_cache_enabled()
    with self._lock:
      token, _ = self._cache.get(state_key)
      if token in [cache_token, None]:
        self._cache.put(state_key, (cache_token, []))
      else:
        # Discard cached state if tokens do not match
        self.evict(state_key)

  @counter("evict")
  def evict(self, state_key):
    assert self.is_cache_enabled()
    with self._lock:
      self._cache.evict(state_key)

  @counter("evict_all")
  def evict_all(self):
    with self._lock:
      self._cache.evict_all()

  def is_cache_enabled(self):
    return self._cache._max_entries > 0

  def size(self):
    return len(self._cache)

  def _get_metrics(self):
    # Initialize metrics without values
    for key in ALL_METRICS:
      if key not in self._metrics:
        self._metrics[key] = 0
    return self._metrics

  def get_monitoring_infos(self):
    """Retrieves the monitoring infos and resets the counters."""
    with self._lock:
      metrics = self._get_metrics()
      # additional gauge metrics
      size = len(self._cache)
      capacity = self._cache._max_entries
      self._metrics = collections.defaultdict(int)
    prefix = "beam:metric:statecache:"
    infos = [monitoring_infos.int64_counter(prefix + name, val)
             for name, val in metrics.items()]
    infos.append(monitoring_infos.int64_gauge(prefix + 'size', size))
    infos.append(monitoring_infos.int64_gauge(prefix + 'capacity', capacity))
    return infos

  class LRUCache(object):

    def __init__(self, max_entries, default_entry):
      self._max_entries = max_entries
      self._default_entry = default_entry
      self._cache = collections.OrderedDict()

    def get(self, key):
      value = self._cache.pop(key, self._default_entry)
      if value != self._default_entry:
        self._cache[key] = value
      return value

    def put(self, key, value):
      self._cache[key] = value
      while len(self._cache) > self._max_entries:
        self._cache.popitem(last=False)

    def evict(self, key):
      self._cache.pop(key, self._default_entry)

    def evict_all(self):
      self._cache.clear()

    def __len__(self):
      return len(self._cache)
