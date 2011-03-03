"""
    Kestrel: An XMPP-based Job Scheduler
    Copyright (C) 2011 Lance Stout
    This file is part of Kestrel.

    See the file LICENSE for copying permission.
"""

from kestrel.config import load_config
from kestrel.worker import Worker
from kestrel.manager import Manager
from kestrel.user import Client

__version__ = '0.9'
