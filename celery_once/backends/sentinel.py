# -*- coding: utf-8 -*-


"""
Definition of the redis sentinel locking backend.
Only Support Python3
PASSWORD/PORT/DB must be same.
"""

from __future__ import absolute_import

from urllib.parse import urlparse, parse_qsl

from celery_once.tasks import AlreadyQueued
from commons import log_utils

logger = log_utils.get_logging('celery_once')


def parse_url(urls):
    """
    Parse the argument urls and return a redis sentinel connection.
    One patterns of url are supported:

        * sentinel://master_name:password@host:port[/db][?options]; ...  type:str

    A ValueError is raised if the urls is not recognized.
    """
    details = {"nodes": []}
    url_list = urls.split(";")
    for url in url_list:
        parsed = urlparse(url)
        kwargs = parse_qsl(parsed.query)

        # TCP redis sentinel connection
        if parsed.scheme == 'sentinel':
            details["nodes"] = details["nodes"] + [[parsed.hostname, parsed.port]]
            if parsed.password:
                if details.get('password') and details.get('password') != parsed.password:
                    raise ValueError("sentinel password is not same!")
                details['password'] = parsed.password
            if parsed.username:
                if details.get('master_name') and details.get('master_name') != parsed.username:
                    raise ValueError("sentinel master_name is not same!")
                details['master_name'] = parsed.username
            db = parsed.path.strip('/')
            if db and db.isdigit():
                if details.get('db') and details.get('db') != int(db):
                    raise ValueError("sentinel db is not same!")
                details['db'] = int(db)
        else:
            raise ValueError('Unsupported protocol %s' % (parsed.scheme))

        # Add kwargs to the details and convert them to the appropriate type, if needed
        details.update(kwargs)
        if 'socket_timeout' in details:
            details['socket_timeout'] = float(details['socket_timeout'])

    return details


sentinel = None

try:
    from redis.lock import Lock
except ImportError:
    raise ImportError(
        "You need to install the redis library in order to use Redis"
        " backend (pip install redis)")


def get_sentinel(nodes):
    global sentinel
    if not sentinel:
        try:
            from redis.sentinel import Sentinel
        except ImportError:
            raise ImportError(
                "You need to install the redis library in order to use Redis"
                " backend (pip install redis)")
        sentinel = Sentinel(nodes)
    return sentinel


class Sentinel(object):
    """Sentinel locking backend."""

    def __init__(self, settings):
        """
        :param settings: eg:{'url': broker_url, 'default_timeout': 60 * 60}
        broker_url: sentinel://master_name:password@host:port[/db][?options]; ...  type:string, split with ;
        """
        parse_dict = parse_url(settings['url'])
        self._sentinel = get_sentinel(parse_dict["nodes"])
        self.blocking_timeout = settings.get("blocking_timeout", 1)
        self.blocking = settings.get("blocking", False)
        self._master = self.sentinel.master_for(parse_dict.get("master_name"), password=parse_dict.get("password"),
                                                socket_timeout=1000, db=parse_dict.get("db"))

    @property
    def sentinel(self):
        # Used to allow easy mocking when testing.
        return self._sentinel

    def raise_or_lock(self, key, timeout):
        """
        Checks if the task is locked and raises an exception, else locks
        the task. By default, the tasks and the key expire after 60 minutes.
        (meaning it will not be executed and the lock will clear).
        """
        master = self._master
        acquired = Lock(
            master,
            key,
            timeout=timeout,
            blocking=self.blocking,
            blocking_timeout=self.blocking_timeout
        ).acquire()

        if not acquired:
            # Time remaining in milliseconds
            # https://redis.io/commands/pttl
            ttl = master.pttl(key)
            raise AlreadyQueued(ttl / 1000.0)

    def clear_lock(self, key):
        """Remove the lock from redis sentinel."""
        return self._master.delete(key)
