
# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#

from __future__ import division
from __future__ import unicode_literals

from mo_threads import Lock


class Counter(object):

    def __init__(self):
        self.locker = Lock()
        self.value = 0

    def __enter__(self):
        with self.locker:
            self.value += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self.locker:
            self.value -= 1
        return self

