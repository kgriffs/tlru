## tLRU

Time-bound LRU caching library for Python 3, optimized for speed and efficient use of resources.

__EXPERIMENTAL__: Use at your own risk.

When installing under PyPy, be sure to:

```
export XXHASH_FORCE_CFFI=1
```

Requires Python 3.5 or better.

Related Works
-------------

The term "TLRU" is also used by M. Bilal and S. Kang in "[Time Aware Least Recent Used (TLRU) cache management policy in ICN][1]," 16th International Conference on Advanced Communication Technology, Pyeongchang, 2014, pp. 528-532, doi: 10.1109/ICACT.2014.6779016.

This Python project was originally implemented independently of M. Bilal and S. Kang's work, and the reuse of the term "TLRU" was coincidental. However, we cite it here as an important related work that describes a higher-level network caching system, while also quantifying the effects of combining a TTL with an LRU eviction strategy.

Legal
-----

Copyright 2018 by individual and corporate contributors as
noted in the individual source files.

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use any portion of this software except in compliance with
the License. Contributors agree to license their work under the same
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[1]: https://arxiv.org/ftp/arxiv/papers/1801/1801.00390.pdf
