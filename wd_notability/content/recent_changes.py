from __future__ import annotations

import sys

from wd_notability.metadata import worker as _impl

sys.modules[__name__] = _impl
